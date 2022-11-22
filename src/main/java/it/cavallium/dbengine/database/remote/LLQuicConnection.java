package it.cavallium.dbengine.database.remote;

import com.google.common.collect.Multimap;
import io.micrometer.core.instrument.MeterRegistry;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.incubator.codec.quic.QuicSslContextBuilder;
import io.netty5.buffer.Buffer;
import io.netty5.buffer.BufferAllocator;
import io.netty5.util.Send;
import it.cavallium.dbengine.client.MemoryStats;
import it.cavallium.dbengine.client.query.current.data.Query;
import it.cavallium.dbengine.client.query.current.data.QueryParams;
import it.cavallium.dbengine.database.ColumnProperty;
import it.cavallium.dbengine.database.LLDatabaseConnection;
import it.cavallium.dbengine.database.LLDelta;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.LLIndexRequest;
import it.cavallium.dbengine.database.LLKeyValueDatabase;
import it.cavallium.dbengine.database.LLLuceneIndex;
import it.cavallium.dbengine.database.LLSearchResultShard;
import it.cavallium.dbengine.database.LLSingleton;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.LLTerm;
import it.cavallium.dbengine.database.LLUpdateDocument;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.RocksDBLongProperty;
import it.cavallium.dbengine.database.RocksDBMapProperty;
import it.cavallium.dbengine.database.RocksDBStringProperty;
import it.cavallium.dbengine.database.TableWithProperties;
import it.cavallium.dbengine.database.UpdateMode;
import it.cavallium.dbengine.database.UpdateReturnMode;
import it.cavallium.dbengine.database.disk.BinarySerializationFunction;
import it.cavallium.dbengine.database.remote.RPCCodecs.RPCEventCodec;
import it.cavallium.dbengine.database.serialization.SerializationException;
import it.cavallium.dbengine.lucene.LuceneHacks;
import it.cavallium.dbengine.lucene.collector.Buckets;
import it.cavallium.dbengine.lucene.searcher.BucketParams;
import it.cavallium.dbengine.rpc.current.data.BinaryOptional;
import it.cavallium.dbengine.rpc.current.data.ClientBoundRequest;
import it.cavallium.dbengine.rpc.current.data.ClientBoundResponse;
import it.cavallium.dbengine.rpc.current.data.CloseDatabase;
import it.cavallium.dbengine.rpc.current.data.CloseLuceneIndex;
import it.cavallium.dbengine.rpc.current.data.Column;
import it.cavallium.dbengine.rpc.current.data.DatabaseOptions;
import it.cavallium.dbengine.rpc.current.data.GeneratedEntityId;
import it.cavallium.dbengine.rpc.current.data.GetDatabase;
import it.cavallium.dbengine.rpc.current.data.GetLuceneIndex;
import it.cavallium.dbengine.rpc.current.data.GetSingleton;
import it.cavallium.dbengine.rpc.current.data.IndicizerAnalyzers;
import it.cavallium.dbengine.rpc.current.data.IndicizerSimilarities;
import it.cavallium.dbengine.rpc.current.data.LuceneIndexStructure;
import it.cavallium.dbengine.rpc.current.data.LuceneOptions;
import it.cavallium.dbengine.rpc.current.data.RPCEvent;
import it.cavallium.dbengine.rpc.current.data.ServerBoundRequest;
import it.cavallium.dbengine.rpc.current.data.ServerBoundResponse;
import it.cavallium.dbengine.rpc.current.data.SingletonGet;
import it.cavallium.dbengine.rpc.current.data.SingletonSet;
import it.cavallium.dbengine.rpc.current.data.SingletonUpdateEnd;
import it.cavallium.dbengine.rpc.current.data.SingletonUpdateInit;
import it.cavallium.dbengine.rpc.current.data.SingletonUpdateOldData;
import it.cavallium.dbengine.rpc.current.data.nullables.NullableBytes;
import it.cavallium.dbengine.rpc.current.data.nullables.NullableLLSnapshot;
import it.unimi.dsi.fastutil.bytes.ByteList;
import java.io.File;
import java.net.SocketAddress;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
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

	private Mono<Void> sendEvent(ServerBoundRequest serverBoundRequest) {
		return QuicUtils.<RPCEvent>sendSimpleEvent(quicConnection,
				RPCEventCodec::new,
				serverBoundRequest
		);
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
					public Mono<Void> ingestSST(Column column, Publisher<Path> files) {
						return null;
					}

					@Override
					public Mono<? extends LLSingleton> getSingleton(byte[] singletonListColumnName,
							byte[] name,
							byte @Nullable[] defaultValue) {
						return sendRequest(new GetSingleton(id,
								ByteList.of(singletonListColumnName),
								ByteList.of(name),
								defaultValue == null ? NullableBytes.empty() : NullableBytes.of(ByteList.of(defaultValue))
						)).cast(GeneratedEntityId.class).map(GeneratedEntityId::id).map(singletonId -> new LLSingleton() {

							@Override
							public BufferAllocator getAllocator() {
								return allocator;
							}

							@Override
							public Mono<Buffer> get(@Nullable LLSnapshot snapshot) {
								return sendRequest(new SingletonGet(singletonId, NullableLLSnapshot.ofNullable(snapshot)))
										.cast(BinaryOptional.class)
										.mapNotNull(result -> {
											if (result.val().isPresent()) {
												return allocator.copyOf(QuicUtils.toArrayNoCopy(result.val().get().val()));
											} else {
												return null;
											}
										});
							}

							@Override
							public Mono<Void> set(Mono<Buffer> valueMono) {
								return QuicUtils.toBytes(valueMono)
										.flatMap(valueSendOpt -> sendRequest(new SingletonSet(singletonId, valueSendOpt)).then());
							}

							@Override
							public Mono<Buffer> update(BinarySerializationFunction updater, UpdateReturnMode updateReturnMode) {
								return LLQuicConnection.this.<BinaryOptional, SingletonUpdateOldData>sendUpdateRequest(new SingletonUpdateInit(singletonId, updateReturnMode), prev -> {
									byte[] oldData = toArrayNoCopy(prev);
									Buffer oldDataBuf;
									if (oldData != null) {
										oldDataBuf = allocator.copyOf(oldData);
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
										return allocator.copyOf(QuicUtils.toArrayNoCopy(result.val().get().val()));
									} else {
										return null;
									}
								});
							}

							@Override
							public Mono<LLDelta> updateAndGetDelta(BinarySerializationFunction updater) {
								return Mono.error(new UnsupportedOperationException());
							}

							@Override
							public String getDatabaseName() {
								return databaseName;
							}

							@Override
							public String getColumnName() {
								return new String(singletonListColumnName);
							}

							@Override
							public String getName() {
								return new String(name);
							}
						});
					}

					@Override
					public Mono<? extends LLDictionary> getDictionary(byte[] columnName, UpdateMode updateMode) {
						return null;
					}

					@Override
					public Mono<MemoryStats> getMemoryStats() {
						return null;
					}

					@Override
					public Mono<String> getRocksDBStats() {
						return null;
					}

					@Override
					public Mono<Long> getAggregatedLongProperty(RocksDBLongProperty property) {
						return null;
					}

					@Override
					public Mono<String> getStringProperty(@Nullable Column column, RocksDBStringProperty property) {
						return null;
					}

					@Override
					public Flux<ColumnProperty<String>> getStringColumnProperties(RocksDBStringProperty property) {
						return null;
					}

					@Override
					public Mono<Long> getLongProperty(@Nullable Column column, RocksDBLongProperty property) {
						return null;
					}

					@Override
					public Flux<ColumnProperty<Long>> getLongColumnProperties(RocksDBLongProperty property) {
						return null;
					}

					@Override
					public Mono<Map<String, String>> getMapProperty(@Nullable Column column, RocksDBMapProperty property) {
						return null;
					}

					@Override
					public Flux<ColumnProperty<Map<String, String>>> getMapColumnProperties(RocksDBMapProperty property) {
						return null;
					}

					@Override
					public Flux<TableWithProperties> getTableProperties() {
						return null;
					}

					@Override
					public Mono<Void> verifyChecksum() {
						return null;
					}

					@Override
					public Mono<Void> compact() {
						return null;
					}

					@Override
					public Mono<Void> flush() {
						return null;
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
					public Mono<Void> preClose() {
						return null;
					}

					@Override
					public Mono<Void> close() {
						return sendRequest(new CloseDatabase(id)).then();
					}

					@Override
					public String getDatabaseName() {
						return databaseName;
					}

					@Override
					public Mono<LLSnapshot> takeSnapshot() {
						return null;
					}

					@Override
					public Mono<Void> releaseSnapshot(LLSnapshot snapshot) {
						return null;
					}

					@Override
					public Mono<Void> pauseForBackup() {
						return Mono.empty();
					}

					@Override
					public Mono<Void> resumeAfterBackup() {
						return Mono.empty();
					}

					@Override
					public boolean isPaused() {
						return false;
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
	public Mono<? extends LLLuceneIndex> getLuceneIndex(String clusterName,
			LuceneIndexStructure indexStructure,
			IndicizerAnalyzers indicizerAnalyzers,
			IndicizerSimilarities indicizerSimilarities,
			LuceneOptions luceneOptions,
			@Nullable LuceneHacks luceneHacks) {
		return sendRequest(new GetLuceneIndex(clusterName, indexStructure, indicizerAnalyzers, indicizerSimilarities, luceneOptions))
				.cast(GeneratedEntityId.class)
				.map(GeneratedEntityId::id)
				.map(id -> new LLLuceneIndex() {

					@Override
					public String getLuceneIndexName() {
						return clusterName;
					}

					@Override
					public Mono<Void> addDocument(LLTerm id, LLUpdateDocument doc) {
						return null;
					}

					@Override
					public Mono<Long> addDocuments(boolean atomic, Flux<Entry<LLTerm, LLUpdateDocument>> documents) {
						return null;
					}

					@Override
					public Mono<Void> deleteDocument(LLTerm id) {
						return null;
					}

					@Override
					public Mono<Void> update(LLTerm id, LLIndexRequest request) {
						return null;
					}

					@Override
					public Mono<Long> updateDocuments(Flux<Entry<LLTerm, LLUpdateDocument>> documents) {
						return null;
					}

					@Override
					public Mono<Void> deleteAll() {
						return null;
					}

					@Override
					public Flux<LLSearchResultShard> moreLikeThis(@Nullable LLSnapshot snapshot,
							QueryParams queryParams,
							@Nullable String keyFieldName,
							Multimap<String, String> mltDocumentFields) {
						return null;
					}

					@Override
					public Flux<LLSearchResultShard> search(@Nullable LLSnapshot snapshot,
							QueryParams queryParams,
							@Nullable String keyFieldName) {
						return null;
					}

					@Override
					public Mono<Buckets> computeBuckets(@Nullable LLSnapshot snapshot,
							@NotNull List<Query> queries,
							@Nullable Query normalizationQuery,
							BucketParams bucketParams) {
						return null;
					}

					@Override
					public boolean isLowMemoryMode() {
						return false;
					}

					@Override
					public void close() {
						sendRequest(new CloseLuceneIndex(id)).then().transform(LLUtils::handleDiscard).block();
					}

					@Override
					public Mono<Void> flush() {
						return null;
					}

					@Override
					public Mono<Void> waitForMerges() {
						return null;
					}

					@Override
					public Mono<Void> waitForLastMerges() {
						return null;
					}

					@Override
					public Mono<Void> refresh(boolean force) {
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

					@Override
					public Mono<Void> pauseForBackup() {
						return null;
					}

					@Override
					public Mono<Void> resumeAfterBackup() {
						return null;
					}

					@Override
					public boolean isPaused() {
						return false;
					}
				});
	}

	@Override
	public Mono<Void> disconnect() {
		return sendDisconnect().then(Mono.fromRunnable(() -> quicConnection.dispose())).then(quicConnection.onDispose());
	}

	private Mono<Void> sendDisconnect() {
		return Mono.empty();
	}
}
