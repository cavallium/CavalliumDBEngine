package it.cavallium.dbengine;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.netty.buffer.api.Buffer;
import io.netty.buffer.api.MemoryManager;
import io.netty.buffer.api.Send;
import io.netty.buffer.api.pool.BufferAllocatorMetric;
import io.netty.buffer.api.pool.PooledBufferAllocator;
import io.netty.util.internal.PlatformDependent;
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
import it.cavallium.dbengine.database.disk.MemorySegmentUtils;
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

	public static record TestAllocator(PooledBufferAllocator allocator) {}

	public static TestAllocator newAllocator() {
		return new TestAllocator(new PooledBufferAllocator(MemoryManager.instance(), true, 1, 8192, 9, 0, 0, false));
	}

	public static void destroyAllocator(TestAllocator testAllocator) {
		testAllocator.allocator().close();
	}

	public static final AtomicInteger dbId = new AtomicInteger(0);

	@SuppressWarnings("SameParameterValue")
	private static long getUsedMemory(PooledBufferAllocator allocator, boolean printStats) {
		allocator.trimCurrentThreadCache();
		var usedMemory = ((BufferAllocatorMetric) allocator.metric()).usedMemory();
		if (printStats) {
			System.out.println("usedMemory=" + usedMemory);
		}
		return usedMemory;
	}

	public static <U> Flux<U> tempDb(TestAllocator alloc, Function<LLKeyValueDatabase, Publisher<U>> action) {
		return Flux.usingWhen(openTempDb(alloc),
				tempDb -> action.apply(tempDb.db()),
				DbTestUtils::closeTempDb
		);
	}

	public static record TempDb(TestAllocator allocator, LLDatabaseConnection connection, LLKeyValueDatabase db,
															Path path) {}

	public static Mono<TempDb> openTempDb(TestAllocator alloc) {
		boolean canUseNettyDirect = computeCanUseNettyDirect();
		return Mono.defer(() -> {
			var wrkspcPath = Path.of("/tmp/.cache/tempdb-" + dbId.incrementAndGet() + "/");
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
					.then(new LLLocalDatabaseConnection(alloc.allocator(), wrkspcPath).connect())
					.flatMap(conn -> conn
							.getDatabase("testdb",
									List.of(Column.dictionary("testmap"), Column.special("ints"), Column.special("longs")),
									new DatabaseOptions(Map.of(), true, false, true, false, true, canUseNettyDirect, canUseNettyDirect, -1)
							)
							.map(db -> new TempDb(alloc, conn, db, wrkspcPath))
					);
		});
	}

	private static boolean computeCanUseNettyDirect() {
		boolean canUse = true;
		if (!PlatformDependent.hasUnsafe()) {
			System.err.println("Warning! Unsafe is not available!"
					+ " Netty direct buffers will not be used in tests!");
			canUse = false;
		}
		if (!MemorySegmentUtils.isSupported()) {
			System.err.println("Warning! Foreign Memory Access API is not available!"
					+ " Netty direct buffers will not be used in tests!"
					+ " Please set \"--enable-preview --add-modules jdk.incubator.foreign -Dforeign.restricted=permit\"");
			if (MemorySegmentUtils.getUnsupportedCause() != null) {
				System.err.println("\tCause: " + MemorySegmentUtils.getUnsupportedCause().getClass().getName()
						+ ":" + MemorySegmentUtils.getUnsupportedCause().getLocalizedMessage());
			}
			canUse = false;
		}
		return canUse;
	}

	public static Mono<Void> closeTempDb(TempDb tempDb) {
		return tempDb.db().close().then(tempDb.connection().disconnect()).then(Mono.fromCallable(() -> {
			ensureNoLeaks(tempDb.allocator().allocator(), false);
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

	public static void ensureNoLeaks(PooledBufferAllocator allocator, boolean printStats) {
		if (allocator != null) {
			assertEquals(0L, getUsedMemory(allocator, printStats));
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
						public @NotNull DeserializationResult<Short> deserialize(@NotNull Send<Buffer> serializedToReceive) {
							try (var serialized = serializedToReceive.receive()) {
								var val = serialized.readShort();
								return new DeserializationResult<>(val, Short.BYTES);
							}
						}

						@Override
						public @NotNull Send<Buffer> serialize(@NotNull Short deserialized) {
							try (var out = dictionary.getAllocator().allocate(Short.BYTES)) {
								out.writeShort(deserialized);
								out.writerOffset(Short.BYTES);
								return out.send();
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
