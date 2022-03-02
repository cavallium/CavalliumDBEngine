package it.cavallium.dbengine.database.remote;

import io.micrometer.core.instrument.MeterRegistry;
import io.net5.buffer.api.Buffer;
import io.net5.buffer.api.BufferAllocator;
import io.net5.buffer.api.Send;
import io.netty.handler.codec.PrematureChannelClosureException;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.incubator.codec.quic.QuicSslContextBuilder;
import it.cavallium.dbengine.client.IndicizerAnalyzers;
import it.cavallium.dbengine.client.IndicizerSimilarities;
import it.cavallium.dbengine.client.LuceneOptions;
import it.cavallium.dbengine.client.MemoryStats;
import it.cavallium.dbengine.database.LLDatabaseConnection;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.LLKeyValueDatabase;
import it.cavallium.dbengine.database.LLLuceneIndex;
import it.cavallium.dbengine.database.LLSingleton;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.UpdateMode;
import it.cavallium.dbengine.database.UpdateReturnMode;
import it.cavallium.dbengine.database.remote.RPCCodecs.RPCClientBoundResponseDecoder;
import it.cavallium.dbengine.database.remote.RPCCodecs.RPCClientAlternateDecoder;
import it.cavallium.dbengine.database.remote.RPCCodecs.RPCServerAlternateDecoder;
import it.cavallium.dbengine.database.remote.RPCCodecs.RPCServerBoundRequestDecoder;
import it.cavallium.dbengine.database.serialization.SerializationFunction;
import it.cavallium.dbengine.lucene.LuceneHacks;
import it.cavallium.dbengine.rpc.current.data.BinaryOptional;
import it.cavallium.dbengine.rpc.current.data.ClientBoundRequest;
import it.cavallium.dbengine.rpc.current.data.ClientBoundResponse;
import it.cavallium.dbengine.rpc.current.data.Column;
import it.cavallium.dbengine.rpc.current.data.DatabaseOptions;
import it.cavallium.dbengine.rpc.current.data.GeneratedEntityId;
import it.cavallium.dbengine.rpc.current.data.GetDatabase;
import it.cavallium.dbengine.rpc.current.data.GetSingleton;
import it.cavallium.dbengine.rpc.current.data.ServerBoundRequest;
import it.cavallium.dbengine.rpc.current.data.ServerBoundResponse;
import it.cavallium.dbengine.rpc.current.data.SingletonGet;
import it.cavallium.dbengine.rpc.current.data.SingletonSet;
import it.cavallium.dbengine.rpc.current.data.nullables.NullableLLSnapshot;
import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.bytes.ByteList;
import java.io.File;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.logging.Level;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;
import reactor.netty.incubator.quic.QuicClient;
import reactor.netty.incubator.quic.QuicConnection;

public class LLQuicConnection implements LLDatabaseConnection {

	private final BufferAllocator allocator;
	private final MeterRegistry meterRegistry;
	private final SocketAddress bindAddress;
	private final SocketAddress remoteAddress;
	private volatile QuicConnection quicConnection;
	private final ConcurrentHashMap<String, Mono<Long>> databases = new ConcurrentHashMap<>();
	private final ConcurrentHashMap<String, Mono<Long>> indexes = new ConcurrentHashMap<>();
	private Mono<Void> connectionMono = Mono.error(new IllegalStateException("Not connected"));

	public LLQuicConnection(BufferAllocator allocator,
			MeterRegistry meterRegistry,
			SocketAddress bindAddress,
			SocketAddress remoteAddress) {
		this.allocator = allocator;
		this.meterRegistry = meterRegistry;
		this.bindAddress = bindAddress;
		this.remoteAddress = remoteAddress;
	}

	@Override
	public BufferAllocator getAllocator() {
		return allocator;
	}

	@Override
	public MeterRegistry getMeterRegistry() {
		return meterRegistry;
	}

	@Override
	public Mono<? extends LLDatabaseConnection> connect() {
		String keyFileLocation = System.getProperty("it.cavalliumdb.keyFile", null);
		String certFileLocation = System.getProperty("it.cavalliumdb.certFile", null);
		String keyStorePassword = System.getProperty("it.cavalliumdb.keyPassword", null);
		String certChainLocation = System.getProperty("it.cavalliumdb.caFile", null);
		File keyFile;
		File certFile;
		File certChain;
		if (keyFileLocation != null) {
			keyFile = new File(keyFileLocation);
		} else {
			keyFile = null;
		}
		if (certFileLocation != null) {
			certFile = new File(certFileLocation);
		} else {
			certFile = null;
		}
		if (certChainLocation != null) {
			certChain = new File(certChainLocation);
		} else {
			certChain = null;
		}
		var sslContextBuilder = QuicSslContextBuilder.forClient();
		if (keyFileLocation != null || certFileLocation != null) {
			sslContextBuilder.keyManager(keyFile, keyStorePassword, certFile);
		}
		if (certChainLocation != null) {
			sslContextBuilder.trustManager(certChain);
		} else {
			sslContextBuilder.trustManager(InsecureTrustManagerFactory.INSTANCE);
		}
		var sslContext = sslContextBuilder
				.applicationProtocols("db/0.9")
				.build();
		return QuicClient.create()
				.bindAddress(() -> bindAddress)
				.remoteAddress(() -> remoteAddress)
				.secure(sslContext)
				.idleTimeout(Duration.ofSeconds(30))
				.initialSettings(spec -> spec
						.maxData(10000000)
						.maxStreamDataBidirectionalLocal(1000000)
				)
				.connect()
				.doOnNext(conn -> quicConnection = conn)
				.thenReturn(this);
	}

	private <T extends ClientBoundResponse> Mono<T> sendRequest(ServerBoundRequest req) {
		return Mono
				.<T>create(sink -> {
					var sub = quicConnection
							.createStream((in, out) -> {
								var writerMono = out
										.withConnection(conn -> conn.addHandler(new RPCServerBoundRequestDecoder()))
										.sendObject(req)
										.then();
								var readerMono = in
										.withConnection(conn -> conn.addHandler(new RPCClientBoundResponseDecoder()))
										.receiveObject()
										.doOnNext(result -> {
											if (result != null) {
												//noinspection unchecked
												sink.success((T) result);
											} else {
												sink.success();
											}
										})
										.take(1, true)
										.singleOrEmpty()
										.then();
								return writerMono
										.then(readerMono)
										.doOnCancel(() -> sink.error(new PrematureChannelClosureException("Request failed")));
							})
							.log("a", Level.INFO)
							.subscribeOn(Schedulers.parallel())
							.subscribe(x -> {}, sink::error);
					sink.onDispose(sub);
				})
				.log("x", Level.INFO);
	}

	private <T extends ClientBoundResponse, U extends ClientBoundRequest> Mono<T> sendUpdateRequest(ServerBoundRequest req,
			Function<U, ServerBoundResponse> updaterFunction) {
		return Mono
				.<T>create(sink -> {
					var sub = quicConnection
							.createStream((in, out) -> {
								var inConn = in.withConnection(conn -> conn.addHandler(new RPCServerAlternateDecoder()));
								var outConn = out.withConnection(conn -> conn.addHandler(new RPCClientAlternateDecoder()));
								var request2 = Sinks.one();
								var writerMono = outConn
										.sendObject(Mono.<Object>just(req).concatWith(request2.asMono()))
										.then();
								var responseMono = inConn
										.receiveObject()
										.switchOnFirst((first, flux) -> {
											//noinspection unchecked
											var req2 = updaterFunction.apply((U) first);
											request2.tryEmitValue(req2);
											//noinspection unchecked
											return flux.skip(1).map(resp2 -> (T) resp2);
										});
								return writerMono
										.thenMany(responseMono)
										.doOnCancel(() -> sink.error(new PrematureChannelClosureException("Request failed")))
										.then();
							})
							.log("a", Level.INFO)
							.subscribeOn(Schedulers.parallel())
							.subscribe(x -> {}, sink::error);
					sink.onDispose(sub);
				})
				.log("x", Level.INFO);
	}

	@Override
	public Mono<? extends LLKeyValueDatabase> getDatabase(String databaseName,
			List<Column> columns, DatabaseOptions databaseOptions) {
		return sendRequest(new GetDatabase(databaseName, columns, databaseOptions))
				.cast(GeneratedEntityId.class)
				.map(GeneratedEntityId::id)
				.map(id -> new LLKeyValueDatabase() {
					@Override
					public Mono<? extends LLSingleton> getSingleton(byte[] singletonListColumnName,
							byte[] name,
							byte[] defaultValue) {
						return sendRequest(new GetSingleton(id,
								ByteList.of(singletonListColumnName),
								ByteList.of(name),
								ByteList.of(defaultValue)
						)).cast(GeneratedEntityId.class).map(GeneratedEntityId::id).map(singletonId -> new LLSingleton() {

							@Override
							public BufferAllocator getAllocator() {
								return allocator;
							}

							@Override
							public Mono<byte[]> get(@Nullable LLSnapshot snapshot) {
								return sendRequest(new SingletonGet(singletonId, NullableLLSnapshot.ofNullable(snapshot)))
										.cast(BinaryOptional.class)
										.mapNotNull(b -> b.val().getNullable())
										.map(binary -> toArrayNoCopy(binary.val()));
							}

							@Override
							public Mono<Void> set(byte[] value) {
								return sendRequest(new SingletonSet(singletonId, ByteList.of(value)))
										.then();
							}

							@Override
							public Mono<Send<Buffer>> update(SerializationFunction<@Nullable Send<Buffer>, @Nullable Buffer> updater,
									UpdateReturnMode updateReturnMode) {
								return null;
							}

							@Override
							public String getDatabaseName() {
								return databaseName;
							}
						});
					}

					@Override
					public Mono<? extends LLDictionary> getDictionary(byte[] columnName, UpdateMode updateMode) {
						return null;
					}

					@Override
					public Mono<Long> getProperty(String propertyName) {
						return null;
					}

					@Override
					public Mono<MemoryStats> getMemoryStats() {
						return null;
					}

					@Override
					public Mono<Void> verifyChecksum() {
						return null;
					}

					@Override
					public BufferAllocator getAllocator() {
						return null;
					}

					@Override
					public MeterRegistry getMeterRegistry() {
						return null;
					}

					@Override
					public Mono<Void> close() {
						return null;
					}

					@Override
					public String getDatabaseName() {
						return null;
					}

					@Override
					public Mono<LLSnapshot> takeSnapshot() {
						return null;
					}

					@Override
					public Mono<Void> releaseSnapshot(LLSnapshot snapshot) {
						return null;
					}
				});
	}

	private static byte[] toArrayNoCopy(ByteList b) {
		if (b instanceof ByteArrayList bal) {
			return bal.elements();
		} else {
			return b.toByteArray();
		}
	}

	@Override
	public Mono<? extends LLLuceneIndex> getLuceneIndex(@Nullable String clusterName,
			@Nullable String shardName,
			int instancesCount,
			IndicizerAnalyzers indicizerAnalyzers,
			IndicizerSimilarities indicizerSimilarities,
			LuceneOptions luceneOptions,
			@Nullable LuceneHacks luceneHacks) {
		return null;
	}

	@Override
	public Mono<Void> disconnect() {
		return sendDisconnect().then(quicConnection.onDispose().timeout(Duration.ofMinutes(1)));
	}

	private Mono<Void> sendDisconnect() {
		return Mono.empty();
	}
}
