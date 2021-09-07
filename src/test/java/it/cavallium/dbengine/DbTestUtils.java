package it.cavallium.dbengine;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.netty5.buffer.api.Buffer;
import io.netty5.buffer.api.MemoryManager;
import io.netty5.buffer.api.Send;
import io.netty5.buffer.api.pool.MetricUtils;
import io.netty5.buffer.api.pool.PoolArenaMetric;
import io.netty5.buffer.api.pool.PooledBufferAllocator;
import io.netty5.util.internal.PlatformDependent;
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
import it.cavallium.dbengine.database.disk.MemorySegmentUtils;
import it.cavallium.dbengine.database.serialization.Serializer;
import it.cavallium.dbengine.database.serialization.SerializerFixedBinaryLength;
import java.nio.file.Path;
import java.util.Map;
import java.util.function.Function;
import org.jetbrains.annotations.NotNull;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class DbTestUtils {


	public static final String BIG_STRING = generateBigString();

	private static String generateBigString() {
		var sb = new StringBuilder();
		for (int i = 0; i < 1024; i++) {
			sb.append("0123456789");
		}
		return sb.toString();
	}

	public static record TestAllocator(PooledBufferAllocator allocator) {}

	public static TestAllocator newAllocator() {
		return new TestAllocator(new PooledBufferAllocator(MemoryManager.instance(), true, 1, 8192, 9, 0, 0, true));
	}

	public static void destroyAllocator(TestAllocator testAllocator) {
		testAllocator.allocator().close();
	}

	@SuppressWarnings("SameParameterValue")
	private static long getActiveAllocations(PooledBufferAllocator allocator, boolean printStats) {
		allocator.trimCurrentThreadCache();
		var metrics = MetricUtils.getPoolArenaMetrics(allocator);
		int allocations = 0;
		int deallocations = 0;
		int activeAllocations = 0;
		for (PoolArenaMetric metric : metrics) {
			allocations += metric.numAllocations();
			deallocations += metric.numDeallocations();
			activeAllocations += metric.numActiveAllocations();
		}
		if (printStats) {
			System.out.println("allocations=" + allocations + ", deallocations=" + deallocations
					+ ", activeAllocations=" + activeAllocations);
		}
		return activeAllocations;
	}

	public static <U> Flux<U> tempDb(TemporaryDbGenerator temporaryDbGenerator,
			TestAllocator alloc,
			Function<LLKeyValueDatabase, Publisher<U>> action) {
		return Flux.usingWhen(
				temporaryDbGenerator.openTempDb(alloc),
				tempDb -> action.apply(tempDb.db()),
				temporaryDbGenerator::closeTempDb
		);
	}

	public static record TempDb(TestAllocator allocator, LLDatabaseConnection connection, LLKeyValueDatabase db,
															Path path) {}

	static boolean computeCanUseNettyDirect() {
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

	public static void ensureNoLeaks(PooledBufferAllocator allocator, boolean printStats, boolean useClassicException) {
		if (allocator != null) {
			var allocs = getActiveAllocations(allocator, printStats);
			if (useClassicException) {
				if (allocs != 0) {
					throw new IllegalStateException("Active allocations: " + allocs);
				}
			} else {
				assertEquals(0L, allocs);
			}
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


	public enum MapType {
		MAP,
		HASH_MAP
	}

	public static DatabaseStageMap<String, String, DatabaseStageEntry<String>> tempDatabaseMapDictionaryMap(
			LLDictionary dictionary,
			MapType mapType,
			int keyBytes) {
		if (mapType == MapType.MAP) {
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
