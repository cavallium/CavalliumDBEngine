package it.cavallium.dbengine;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.netty5.buffer.api.Buffer;
import io.netty5.buffer.api.MemoryManager;
import io.netty5.buffer.api.pool.PoolArenaMetric;
import io.netty5.buffer.api.pool.PooledBufferAllocator;
import io.netty5.util.internal.PlatformDependent;
import it.cavallium.dbengine.client.LuceneIndex;
import it.cavallium.dbengine.client.LuceneIndexImpl;
import it.cavallium.dbengine.database.LLDatabaseConnection;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.LLKeyValueDatabase;
import it.cavallium.dbengine.database.LLLuceneIndex;
import it.cavallium.dbengine.database.UpdateMode;
import it.cavallium.dbengine.database.collections.DatabaseMapDictionary;
import it.cavallium.dbengine.database.collections.DatabaseMapDictionaryDeep;
import it.cavallium.dbengine.database.collections.DatabaseMapDictionaryHashed;
import it.cavallium.dbengine.database.collections.DatabaseStageEntry;
import it.cavallium.dbengine.database.collections.DatabaseStageMap;
import it.cavallium.dbengine.database.collections.SubStageGetterHashMap;
import it.cavallium.dbengine.database.collections.SubStageGetterMap;
import it.cavallium.dbengine.database.serialization.Serializer;
import it.cavallium.dbengine.database.serialization.SerializerFixedBinaryLength;
import it.unimi.dsi.fastutil.objects.Object2ObjectSortedMap;
import java.nio.file.Path;
import java.util.Objects;
import java.util.function.Function;
import org.jetbrains.annotations.NotNull;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class DbTestUtils {

	public static final String BIG_STRING = generateBigString();
	public static final int MAX_IN_MEMORY_RESULT_ENTRIES = 8192;

	private static String generateBigString() {
		return "0123456789".repeat(1024);
	}

	public record TestAllocator(PooledBufferAllocator allocator) {}

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

	public static boolean isCIMode() {
		return System.getProperty("dbengine.ci", "false").equalsIgnoreCase("true");
	}

	public static <U> Flux<U> tempDb(TemporaryDbGenerator temporaryDbGenerator,
			TestAllocator alloc,
			Function<LLKeyValueDatabase, Publisher<U>> action) {
		return Flux.usingWhen(
				temporaryDbGenerator.openTempDb(alloc),
				tempDb -> Flux
						.from(action.apply(tempDb.db()))
						.doOnDiscard(Object.class, o -> System.out.println("Discarded: " + o.getClass().getName() + ", " + o)),
				temporaryDbGenerator::closeTempDb
		);
	}

	public record TempDb(TestAllocator allocator, LLDatabaseConnection connection, LLKeyValueDatabase db,
											 LLLuceneIndex luceneSingle,
											 LLLuceneIndex luceneMulti,
											 SwappableLuceneSearcher swappableLuceneSearcher,
											 Path path) {}

	static boolean computeCanUseNettyDirect() {
		boolean canUse = true;
		if (!PlatformDependent.hasUnsafe()) {
			System.err.println("Warning! Unsafe is not available!"
					+ " Netty direct buffers will not be used in tests!");
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

	public static Mono<? extends LuceneIndex<String, String>> tempLuceneIndex(LLLuceneIndex index) {
		return Mono.fromCallable(() -> new LuceneIndexImpl<>(index, new StringIndicizer()));
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
					SerializerFixedBinaryLength.utf8(keyBytes),
					Serializer.UTF8_SERIALIZER,
					null
			);
		} else {
			return DatabaseMapDictionaryHashed.simple(dictionary,
					Serializer.UTF8_SERIALIZER,
					Serializer.UTF8_SERIALIZER,
					s -> (short) s.hashCode(),
					new SerializerFixedBinaryLength<>() {
						@Override
						public int getSerializedBinaryLength() {
							return Short.BYTES;
						}

						@Override
						public @NotNull Short deserialize(@NotNull Buffer serialized) {
							Objects.requireNonNull(serialized);
							return serialized.readShort();
						}

						@Override
						public void serialize(@NotNull Short deserialized, Buffer output) {
							output.writeShort(deserialized);
						}
					},
					null
			);
		}
	}

	public static DatabaseMapDictionaryDeep<String, Object2ObjectSortedMap<String, String>,
			DatabaseMapDictionary<String, String>> tempDatabaseMapDictionaryDeepMap(
			LLDictionary dictionary,
			int key1Bytes,
			int key2Bytes) {
		return DatabaseMapDictionaryDeep.deepTail(dictionary,
				SerializerFixedBinaryLength.utf8(key1Bytes),
				key2Bytes,
				new SubStageGetterMap<>(SerializerFixedBinaryLength.utf8(key2Bytes),
						Serializer.UTF8_SERIALIZER
				),
				null
		);
	}

	public static DatabaseMapDictionaryDeep<String, Object2ObjectSortedMap<String, String>,
			DatabaseMapDictionaryHashed<String, String, Integer>> tempDatabaseMapDictionaryDeepMapHashMap(
			LLDictionary dictionary,
			int key1Bytes) {
		return DatabaseMapDictionaryDeep.deepTail(dictionary,
				SerializerFixedBinaryLength.utf8(key1Bytes),
				Integer.BYTES,
				new SubStageGetterHashMap<>(Serializer.UTF8_SERIALIZER,
						Serializer.UTF8_SERIALIZER,
						String::hashCode,
						SerializerFixedBinaryLength.intSerializer(dictionary.getAllocator())
				),
				null
		);
	}

	public static <T, U> DatabaseMapDictionaryHashed<String, String, Integer> tempDatabaseMapDictionaryHashMap(
			LLDictionary dictionary) {
		return DatabaseMapDictionaryHashed.simple(dictionary,
				Serializer.UTF8_SERIALIZER,
				Serializer.UTF8_SERIALIZER,
				String::hashCode,
				SerializerFixedBinaryLength.intSerializer(dictionary.getAllocator()),
				null
		);
	}
}
