package it.cavallium.dbengine;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PoolArenaMetric;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import it.cavallium.dbengine.database.Column;
import it.cavallium.dbengine.database.LLDatabaseConnection;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.LLKeyValueDatabase;
import it.cavallium.dbengine.database.UpdateMode;
import it.cavallium.dbengine.database.collections.DatabaseMapDictionary;
import it.cavallium.dbengine.database.collections.DatabaseMapDictionaryDeep;
import it.cavallium.dbengine.database.collections.DatabaseMapDictionaryHashed;
import it.cavallium.dbengine.database.collections.DatabaseStageEntry;
import it.cavallium.dbengine.database.collections.DatabaseStageMap;
import it.cavallium.dbengine.database.collections.SubStageGetterHashMap;
import it.cavallium.dbengine.database.collections.SubStageGetterMap;
import it.cavallium.dbengine.client.DatabaseOptions;
import it.cavallium.dbengine.database.disk.LLLocalDatabaseConnection;
import it.cavallium.dbengine.database.serialization.Serializer;
import it.cavallium.dbengine.database.serialization.SerializerFixedBinaryLength;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.jetbrains.annotations.NotNull;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class DbTestUtils {

	private volatile static ByteBufAllocator POOLED_ALLOCATOR = null;

	public static synchronized ByteBufAllocator getUncachedAllocator() {
		try {
			ensureNoLeaks(POOLED_ALLOCATOR);
		} catch (Throwable ex) {
			POOLED_ALLOCATOR = null;
		}
		if (POOLED_ALLOCATOR == null) {
			POOLED_ALLOCATOR = new PooledByteBufAllocator(false, 1, 0, 8192, 11, 0, 0, true);
		}
		return POOLED_ALLOCATOR;
	}

	public static synchronized ByteBufAllocator getUncachedAllocatorUnsafe() {
		return POOLED_ALLOCATOR;
	}

	public static final AtomicInteger dbId = new AtomicInteger(0);

	@SuppressWarnings("SameParameterValue")
	private static int getActiveBuffers(ByteBufAllocator allocator) {
		int directActive = 0, directAlloc = 0, directDealloc = 0;
		if (allocator instanceof PooledByteBufAllocator alloc) {
			for (PoolArenaMetric arena : alloc.directArenas()) {
				directActive += arena.numActiveAllocations();
				directAlloc += arena.numAllocations();
				directDealloc += arena.numDeallocations();
			}
		} else if (allocator instanceof UnpooledByteBufAllocator alloc) {
			directActive += alloc.metric().usedDirectMemory();
		} else {
			throw new UnsupportedOperationException();
		}
		System.out.println("directActive " + directActive + " directAlloc " + directAlloc + " directDealloc " + directDealloc);
		return directActive;
	}

	@SuppressWarnings("SameParameterValue")
	private static int getActiveHeapBuffers(ByteBufAllocator allocator) {
		int heapActive = 0, heapAlloc = 0, heapDealloc = 0;
		if (allocator instanceof PooledByteBufAllocator alloc) {
			for (PoolArenaMetric arena : alloc.heapArenas()) {
				heapActive += arena.numActiveAllocations();
				heapAlloc += arena.numAllocations();
				heapDealloc += arena.numDeallocations();
			}
		} else if (allocator instanceof UnpooledByteBufAllocator alloc) {
			heapActive += alloc.metric().usedHeapMemory();
		} else {
			throw new UnsupportedOperationException();
		}
		System.out.println("heapActive " + heapActive + " heapAlloc " + heapAlloc + " heapDealloc " + heapDealloc);
		return heapActive;
	}

	public static <U> Flux<U> tempDb(Function<LLKeyValueDatabase, Publisher<U>> action) {
		return Flux.usingWhen(openTempDb(),
				tempDb -> action.apply(tempDb.db()),
				DbTestUtils::closeTempDb
		);
	}

	public static record TempDb(ByteBufAllocator allocator, LLDatabaseConnection connection, LLKeyValueDatabase db,
															Path path) {}

	public static Mono<TempDb> openTempDb() {
		return Mono.defer(() -> {
			var wrkspcPath = Path.of("/tmp/.cache/tempdb-" + dbId.incrementAndGet() + "/");
			var alloc = getUncachedAllocator();
			return Mono
					.<LLKeyValueDatabase>fromCallable(() -> {
						if (Files.exists(wrkspcPath)) {
							Files.walk(wrkspcPath).sorted(Comparator.reverseOrder()).forEach(file -> {
								try {
									Files.delete(file);
								} catch (IOException ex) {
									throw new CompletionException(ex);
								}
							});
						}
						Files.createDirectories(wrkspcPath);
						return null;
					})
					.subscribeOn(Schedulers.boundedElastic())
					.then(new LLLocalDatabaseConnection(alloc, wrkspcPath).connect())
					.flatMap(conn -> conn
							.getDatabase("testdb",
									List.of(Column.dictionary("testmap"), Column.special("ints"), Column.special("longs")),
									new DatabaseOptions(Map.of(), true, false, true, false, true, true, true, -1)
							)
							.map(db -> new TempDb(alloc, conn, db, wrkspcPath))
					);
		});
	}

	public static Mono<Void> closeTempDb(TempDb tempDb) {
		return tempDb.db().close().then(tempDb.connection().disconnect()).then(Mono.fromCallable(() -> {
			ensureNoLeaks(tempDb.allocator());
			if (tempDb.allocator() instanceof PooledByteBufAllocator pooledByteBufAllocator) {
				pooledByteBufAllocator.trimCurrentThreadCache();
				pooledByteBufAllocator.freeThreadLocalCache();
			}
			if (Files.exists(tempDb.path())) {
				Files.walk(tempDb.path()).sorted(Comparator.reverseOrder()).forEach(file -> {
					try {
						Files.delete(file);
					} catch (IOException ex) {
						throw new CompletionException(ex);
					}
				});
			}
			return null;
		}).subscribeOn(Schedulers.boundedElastic())).then();
	}

	public static void ensureNoLeaks(ByteBufAllocator allocator) {
		if (allocator != null) {
			assertEquals(0, getActiveBuffers(allocator));
			assertEquals(0, getActiveHeapBuffers(allocator));
		}
	}

	public static Mono<? extends LLDictionary> tempDictionary(LLKeyValueDatabase database, UpdateMode updateMode) {
		return tempDictionary(database, "testmap", updateMode);
	}

	public static Mono<? extends LLDictionary> tempDictionary(LLKeyValueDatabase database,
			String name,
			UpdateMode updateMode) {
		return database.getDictionary(name, updateMode);
	}


	public enum DbType {
		MAP,
		HASH_MAP
	}

	public static DatabaseStageMap<String, String, DatabaseStageEntry<String>> tempDatabaseMapDictionaryMap(
			LLDictionary dictionary,
			DbType dbType,
			int keyBytes) {
		if (dbType == DbType.MAP) {
			return DatabaseMapDictionary.simple(dictionary,
					SerializerFixedBinaryLength.utf8(dictionary.getAllocator(), keyBytes),
					Serializer.utf8(dictionary.getAllocator())
			);
		} else {
			return DatabaseMapDictionaryHashed.simple(dictionary,
					Serializer.utf8(dictionary.getAllocator()),
					Serializer.utf8(dictionary.getAllocator()),
					s -> (short) s.hashCode(),
					new SerializerFixedBinaryLength<>() {
						@Override
						public int getSerializedBinaryLength() {
							return Short.BYTES;
						}

						@Override
						public @NotNull Short deserialize(@NotNull ByteBuf serialized) {
							try {
								var prevReaderIdx = serialized.readerIndex();
								var val = serialized.readShort();
								serialized.readerIndex(prevReaderIdx + Short.BYTES);
								return val;
							} finally {
								serialized.release();
							}
						}

						@Override
						public @NotNull ByteBuf serialize(@NotNull Short deserialized) {
							var out = dictionary.getAllocator().directBuffer(Short.BYTES);
							try {
								out.writeShort(deserialized);
								out.writerIndex(Short.BYTES);
								return out.retain();
							} finally {
								out.release();
							}
						}
					}
			);
		}
	}

	public static DatabaseMapDictionaryDeep<String, Map<String, String>,
			DatabaseMapDictionary<String, String>> tempDatabaseMapDictionaryDeepMap(
			LLDictionary dictionary,
			int key1Bytes,
			int key2Bytes) {
		return DatabaseMapDictionaryDeep.deepTail(dictionary,
				SerializerFixedBinaryLength.utf8(dictionary.getAllocator(), key1Bytes),
				key2Bytes,
				new SubStageGetterMap<>(SerializerFixedBinaryLength.utf8(dictionary.getAllocator(), key2Bytes),
						Serializer.utf8(dictionary.getAllocator())
				)
		);
	}

	public static DatabaseMapDictionaryDeep<String, Map<String, String>,
			DatabaseMapDictionaryHashed<String, String, Integer>> tempDatabaseMapDictionaryDeepMapHashMap(
			LLDictionary dictionary,
			int key1Bytes) {
		return DatabaseMapDictionaryDeep.deepTail(dictionary,
				SerializerFixedBinaryLength.utf8(dictionary.getAllocator(), key1Bytes),
				Integer.BYTES,
				new SubStageGetterHashMap<>(Serializer.utf8(dictionary.getAllocator()),
						Serializer.utf8(dictionary.getAllocator()),
						String::hashCode,
						SerializerFixedBinaryLength.intSerializer(dictionary.getAllocator())
				)
		);
	}

	public static <T, U> DatabaseMapDictionaryHashed<String, String, Integer> tempDatabaseMapDictionaryHashMap(
			LLDictionary dictionary) {
		return DatabaseMapDictionaryHashed.simple(dictionary,
				Serializer.utf8(dictionary.getAllocator()),
				Serializer.utf8(dictionary.getAllocator()),
				String::hashCode,
				SerializerFixedBinaryLength.intSerializer(dictionary.getAllocator())
		);
	}
}
