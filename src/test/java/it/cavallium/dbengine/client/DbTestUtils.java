package it.cavallium.dbengine.client;

import it.cavallium.dbengine.database.Column;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.LLKeyValueDatabase;
import it.cavallium.dbengine.database.UpdateMode;
import it.cavallium.dbengine.database.collections.DatabaseMapDictionary;
import it.cavallium.dbengine.database.collections.DatabaseMapDictionaryDeep;
import it.cavallium.dbengine.database.collections.DatabaseMapDictionaryHashed;
import it.cavallium.dbengine.database.collections.SubStageGetterHashMap;
import it.cavallium.dbengine.database.collections.SubStageGetterMap;
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
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class DbTestUtils {

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
						.then(new LLLocalDatabaseConnection(wrkspcPath, true).connect())
						.flatMap(conn -> conn.getDatabase("testdb",
								List.of(Column.dictionary("testmap"), Column.special("ints"), Column.special("longs")),
								false, true
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

	public static DatabaseMapDictionary<String, String> tempDatabaseMapDictionaryMap(
			LLDictionary dictionary,
			int keyBytes) {
		return DatabaseMapDictionary.simple(dictionary, SerializerFixedBinaryLength.utf8(keyBytes), Serializer.utf8());
	}

	public static <T, U> DatabaseMapDictionaryDeep<String, Map<String, String>, DatabaseMapDictionary<String, String>> tempDatabaseMapDictionaryDeepMap(
			LLDictionary dictionary,
			int key1Bytes,
			int key2Bytes) {
		return DatabaseMapDictionaryDeep.deepTail(dictionary,
				SerializerFixedBinaryLength.utf8(key1Bytes),
				key2Bytes,
				new SubStageGetterMap<>(SerializerFixedBinaryLength.utf8(key2Bytes), Serializer.UTF8_SERIALIZER)
		);
	}

	public static <T, U> DatabaseMapDictionaryHashed<String, String, Integer> tempDatabaseMapDictionaryHashMap(
			LLDictionary dictionary) {
		return DatabaseMapDictionaryHashed.simple(dictionary,
				Serializer.utf8(),
				Serializer.utf8(),
				String::hashCode,
				SerializerFixedBinaryLength.intSerializer()
		);
	}
}
