package it.cavallium.dbengine.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;

import it.cavallium.dbengine.buffers.BufDataInput;
import it.cavallium.dbengine.buffers.BufDataOutput;
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
import it.cavallium.dbengine.database.serialization.SerializationException;
import it.cavallium.dbengine.database.serialization.Serializer;
import it.cavallium.dbengine.database.serialization.SerializerFixedBinaryLength;
import it.unimi.dsi.fastutil.objects.Object2ObjectSortedMap;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.lucene.util.IOSupplier;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;

public class DbTestUtils {

	public static final String BIG_STRING = generateBigString();
	public static final int MAX_IN_MEMORY_RESULT_ENTRIES = 8192;

	private static String generateBigString() {
		return "0123456789".repeat(1024);
	}

	public static boolean isCIMode() {
		return System.getProperty("dbengine.ci", "false").equalsIgnoreCase("true");
	}

	public static <U> U tempDb(TemporaryDbGenerator temporaryDbGenerator,
			Function<LLKeyValueDatabase, U> action) throws IOException {
		var tempDb = temporaryDbGenerator.openTempDb();
		try {
			return action.apply(tempDb.db());
		} finally {
			temporaryDbGenerator.closeTempDb(tempDb);
		}
	}

	public static void runVoid(boolean shouldFail, Runnable consumer) {
		if (shouldFail) {
			Assertions.assertThrows(Throwable.class, consumer::run);
		} else {
			Assertions.assertDoesNotThrow(consumer::run);
		}
	}

	public static <X> X run(boolean shouldFail, IOSupplier<X> consumer) {
		AtomicReference<X> result = new AtomicReference<>(null);
		if (shouldFail) {
			Assertions.assertThrows(Throwable.class, consumer::get);
		} else {
			Assertions.assertDoesNotThrow(() -> result.set(consumer.get()));
		}
		return result.get();
	}

	public record TempDb(LLDatabaseConnection connection, LLKeyValueDatabase db,
											 LLLuceneIndex luceneSingle,
											 LLLuceneIndex luceneMulti,
											 SwappableLuceneSearcher swappableLuceneSearcher,
											 Path path) {}

	public static void ensureNoLeaks() {
		System.gc();
	}

	public static LLDictionary tempDictionary(LLKeyValueDatabase database, UpdateMode updateMode) {
		return tempDictionary(database, "testmap", updateMode);
	}

	public static LLDictionary tempDictionary(LLKeyValueDatabase database,
			String name,
			UpdateMode updateMode) {
		return database.getDictionary(name, updateMode);
	}

	public static LuceneIndex<String, String> tempLuceneIndex(LLLuceneIndex index) {
		return new LuceneIndexImpl<>(index, new StringIndicizer());
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
					Serializer.UTF8_SERIALIZER
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
						public @NotNull Short deserialize(@NotNull BufDataInput in) throws SerializationException {
							return in.readShort();
						}

						@Override
						public void serialize(@NotNull Short deserialized, BufDataOutput out) throws SerializationException {
							out.writeShort(deserialized);
						}
					}
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
				)
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
						SerializerFixedBinaryLength.intSerializer()
				)
		);
	}

	public static DatabaseMapDictionaryHashed<String, String, Integer> tempDatabaseMapDictionaryHashMap(
			LLDictionary dictionary) {
		return DatabaseMapDictionaryHashed.simple(dictionary,
				Serializer.UTF8_SERIALIZER,
				Serializer.UTF8_SERIALIZER,
				String::hashCode,
				SerializerFixedBinaryLength.intSerializer()
		);
	}
}
