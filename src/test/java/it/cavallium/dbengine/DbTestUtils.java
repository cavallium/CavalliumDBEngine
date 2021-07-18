package it.cavallium.dbengine;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import it.cavallium.dbengine.database.Column;
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

	public static final ByteBufAllocator ALLOCATOR = new PooledByteBufAllocator(true);
	public static final AtomicInteger dbId = new AtomicInteger(0);

	public static <U> Flux<U> tempDb(Function<LLKeyValueDatabase, Publisher<U>> action) {
		var wrkspcPath = Path.of("/tmp/.cache/tempdb-" + dbId.incrementAndGet() + "/");
		return Flux.usingWhen(Mono
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
						.then(new LLLocalDatabaseConnection(DbTestUtils.ALLOCATOR, wrkspcPath).connect())
						.flatMap(conn -> conn.getDatabase("testdb",
								List.of(Column.dictionary("testmap"), Column.special("ints"), Column.special("longs")),
								new DatabaseOptions(Map.of(), true, false, true, false, true, true, true, true)
						)),
				action,
				db -> db.close().then(Mono.fromCallable(() -> {
					if (Files.exists(wrkspcPath)) {
						Files.walk(wrkspcPath).sorted(Comparator.reverseOrder()).forEach(file -> {
							try {
								Files.delete(file);
							} catch (IOException ex) {
								throw new CompletionException(ex);
							}
						});
					}
					return null;
				}).subscribeOn(Schedulers.boundedElastic()))
		);
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
					SerializerFixedBinaryLength.utf8(DbTestUtils.ALLOCATOR, keyBytes),
					Serializer.utf8(DbTestUtils.ALLOCATOR)
			);
		} else {
			return DatabaseMapDictionaryHashed.simple(dictionary,
					Serializer.utf8(DbTestUtils.ALLOCATOR),
					Serializer.utf8(DbTestUtils.ALLOCATOR),
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
							var out = DbTestUtils.ALLOCATOR.directBuffer(Short.BYTES);
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

	public static <T, U> DatabaseMapDictionaryDeep<String, Map<String, String>,
			DatabaseMapDictionary<String, String>> tempDatabaseMapDictionaryDeepMap(
			LLDictionary dictionary,
			int key1Bytes,
			int key2Bytes) {
		return DatabaseMapDictionaryDeep.deepTail(dictionary,
				SerializerFixedBinaryLength.utf8(DbTestUtils.ALLOCATOR, key1Bytes),
				key2Bytes,
				new SubStageGetterMap<>(SerializerFixedBinaryLength.utf8(DbTestUtils.ALLOCATOR, key2Bytes),
						Serializer.utf8(DbTestUtils.ALLOCATOR),
						true
				)
		);
	}

	public static <T, U> DatabaseMapDictionaryDeep<String, Map<String, String>,
			DatabaseMapDictionaryHashed<String, String, Integer>> tempDatabaseMapDictionaryDeepMapHashMap(
			LLDictionary dictionary,
			int key1Bytes) {
		return DatabaseMapDictionaryDeep.deepTail(dictionary,
				SerializerFixedBinaryLength.utf8(DbTestUtils.ALLOCATOR, key1Bytes),
				Integer.BYTES,
				new SubStageGetterHashMap<>(Serializer.utf8(DbTestUtils.ALLOCATOR),
						Serializer.utf8(DbTestUtils.ALLOCATOR),
						String::hashCode,
						SerializerFixedBinaryLength.intSerializer(DbTestUtils.ALLOCATOR),
						true
				)
		);
	}

	public static <T, U> DatabaseMapDictionaryHashed<String, String, Integer> tempDatabaseMapDictionaryHashMap(
			LLDictionary dictionary) {
		return DatabaseMapDictionaryHashed.simple(dictionary,
				Serializer.utf8(DbTestUtils.ALLOCATOR),
				Serializer.utf8(DbTestUtils.ALLOCATOR),
				String::hashCode,
				SerializerFixedBinaryLength.intSerializer(DbTestUtils.ALLOCATOR)
		);
	}
}
