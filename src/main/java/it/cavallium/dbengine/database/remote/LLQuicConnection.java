package it.cavallium.dbengine.database.remote;

import io.micrometer.core.instrument.MeterRegistry;
import io.net5.buffer.api.Buffer;
import io.net5.buffer.api.BufferAllocator;
import io.net5.buffer.api.Send;
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
import it.cavallium.dbengine.database.remote.RPCCodecs.RPCEventCodec;
import it.cavallium.dbengine.database.serialization.SerializationException;
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
import it.cavallium.dbengine.rpc.current.data.RPCEvent;
import it.cavallium.dbengine.rpc.current.data.ServerBoundRequest;
import it.cavallium.dbengine.rpc.current.data.ServerBoundResponse;
import it.cavallium.dbengine.rpc.current.data.SingletonGet;
import it.cavallium.dbengine.rpc.current.data.SingletonSet;
import it.cavallium.dbengine.rpc.current.data.SingletonUpdateEnd;
import it.cavallium.dbengine.rpc.current.data.SingletonUpdateInit;
import it.cavallium.dbengine.rpc.current.data.SingletonUpdateOldData;
import it.cavallium.dbengine.rpc.current.data.nullables.NullableLLSnapshot;
import it.unimi.dsi.fastutil.bytes.ByteList;
import java.io.File;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.logging.Level;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.Empty;
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

	@SuppressWarnings("unchecked")
	private <T extends ClientBoundResponse> Mono<T> sendRequest(ServerBoundRequest serverBoundRequest) {
		return QuicUtils.<RPCEvent, RPCEvent>sendSimpleRequest(quicConnection,
				RPCEventCodec::new,
				RPCEventCodec::new,
				serverBoundRequest
		).map(event -> (T) event);
	}

	private <T extends ClientBoundResponse, U extends ClientBoundRequest> Mono<T> sendUpdateRequest(ServerBoundRequest serverBoundReq,
			Function<U, ServerBoundResponse> updaterFunction) {
		return Mono.empty();
			/*

		return Mono.defer(() -> {
			Empty<Void> streamTerminator = Sinks.empty();
			return QuicUtils.createStream(quicConnection, stream -> {
				Mono<Void> serverReq = Mono.defer(() -> stream.out()
						.withConnection(conn -> conn.addHandler(new RPCCodecs.RPCServerBoundRequestDecoder()))
						.sendObject(serverBoundReq)
						.then()).doOnSubscribe(s -> System.out.println("out1"));
				//noinspection unchecked
				Mono<U> clientBoundReqMono = Mono.defer(() -> stream.in()
						.withConnection(conn -> conn.addHandler(new RPCClientBoundRequestDecoder()))
						.receiveObject()
						.log("TO_CLIENT_REQ", Level.INFO)
						.take(1, true)
						.singleOrEmpty()
						.map(req -> (U) req)
						.doOnSubscribe(s -> System.out.println("in1"))
						.switchIfEmpty((Mono<U>) QuicUtils.NO_RESPONSE_ERROR)
				);
				Mono<Void> serverBoundRespFlux = clientBoundReqMono
						.map(updaterFunction)
						.transform(respMono -> Mono.defer(() -> stream.out()
								.withConnection(conn -> conn.addHandler(new RPCServerBoundResponseDecoder()))
								.sendObject(respMono)
								.then()
								.doOnSubscribe(s -> System.out.println("out2"))
						));
				//noinspection unchecked
				Mono<T> clientBoundResponseMono = Mono.defer(() -> stream.in()
						.withConnection(conn -> conn.addHandler(new RPCClientBoundResponseDecoder()))
						.receiveObject()
						.map(resp -> (T) resp)
						.log("TO_SERVER_RESP", Level.INFO)
						.take(1, true)
						.doOnSubscribe(s -> System.out.println("out2"))
						.singleOrEmpty()
						.switchIfEmpty((Mono<T>) QuicUtils.NO_RESPONSE_ERROR));
				return serverReq
						.then(serverBoundRespFlux)
						.then(clientBoundResponseMono)
						.doFinally(s -> streamTerminator.tryEmitEmpty());
			}, streamTerminator.asMono()).single();
		});

			 */
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
										.map(binary -> QuicUtils.toArrayNoCopy(binary.val()));
							}

							@Override
							public Mono<Void> set(byte[] value) {
								return sendRequest(new SingletonSet(singletonId, ByteList.of(value)))
										.then();
							}

							@Override
							public Mono<Send<Buffer>> update(SerializationFunction<@Nullable Send<Buffer>, @Nullable Buffer> updater,
									UpdateReturnMode updateReturnMode) {
								return LLQuicConnection.this.<BinaryOptional, SingletonUpdateOldData>sendUpdateRequest(new SingletonUpdateInit(singletonId, updateReturnMode), prev -> {
									byte[] oldData = toArrayNoCopy(prev);
									Send<Buffer> oldDataBuf;
									if (oldData != null) {
										oldDataBuf = allocator.copyOf(oldData).send();
									} else {
										oldDataBuf = null;
									}
									try (oldDataBuf) {
										try (var result = updater.apply(oldDataBuf)) {
											if (result == null) {
												return new SingletonUpdateEnd(false, ByteList.of());
											} else {
												byte[] resultArray = new byte[result.readableBytes()];
												result.readBytes(resultArray, 0, resultArray.length);
												return new SingletonUpdateEnd(true, ByteList.of(resultArray));
											}
										}
									} catch (SerializationException e) {
										throw new IllegalStateException(e);
									}
								}).mapNotNull(result -> {
									if (result.val().isPresent()) {
										return allocator.copyOf(QuicUtils.toArrayNoCopy(result.val().get().val())).send();
									} else {
										return null;
									}
								});
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

	@Nullable
	private static byte[] toArrayNoCopy(SingletonUpdateOldData oldData) {
		if (oldData.exist()) {
			return QuicUtils.toArrayNoCopy(oldData.oldValue());
		} else {
			return null;
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
