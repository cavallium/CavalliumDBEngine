package it.cavallium.dbengine.client;

import static it.cavallium.dbengine.client.CompositeDatabasePartLocation.CompositeDatabasePartType.KV_DATABASE;

import it.cavallium.dbengine.database.Column;
import it.cavallium.dbengine.database.LLKeyValueDatabase;
import it.cavallium.dbengine.database.UpdateMode;
import it.cavallium.dbengine.database.collections.DatabaseMapDictionaryDeep;
import it.cavallium.dbengine.database.collections.SubStageGetterMap;
import it.cavallium.dbengine.database.disk.LLLocalDatabaseConnection;
import it.cavallium.dbengine.database.serialization.Serializer;
import it.cavallium.dbengine.database.serialization.SerializerFixedBinaryLength;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.util.function.Tuples;

public class Database {

	@Test
	public void testDeepDatabaseAddKeysAndConvertToLongerOnes() {
		LinkedHashSet<String> originalSuperKeys = new LinkedHashSet<>(List.of("K1a", "K1b", "K1c"));
		LinkedHashSet<String> originalSubKeys = new LinkedHashSet<>(List.of("K2aa", "K2bb", "K2cc"));
		String newPrefix = "xxx";

		StepVerifier
				.create(
						tempDb()
								.flatMapMany(db -> addKeysAndConvertToLongerOnes(db, originalSuperKeys, originalSubKeys, newPrefix))
				)
				.expectNextSequence(originalSuperKeys
						.stream()
						.flatMap(superKey -> originalSubKeys
								.stream()
								.map(subKey -> Map.entry(newPrefix + superKey, newPrefix + subKey)))
						.collect(Collectors.toList())
				)
				.verifyComplete();
	}

	public static <U> Mono<? extends LLKeyValueDatabase> tempDb() {
		var wrkspcPath = Path.of("/tmp/.cache/tempdb/");
		return Mono
				.fromCallable(() -> {
					if (Files.exists(wrkspcPath)) {
						Files.walk(wrkspcPath)
								.sorted(Comparator.reverseOrder())
								.forEach(file -> {
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
				.flatMap(conn -> conn.getDatabase("testdb", List.of(Column.dictionary("testmap")), false));
	}

	private static final byte[] DUMMY_VALUE = new byte[] {0x01, 0x03};

	private Flux<Entry<String, String>> addKeysAndConvertToLongerOnes(LLKeyValueDatabase db,
			LinkedHashSet<String> originalSuperKeys,
			LinkedHashSet<String> originalSubKeys,
			String newPrefix) {
		return Flux
				.defer(() -> Mono
						.zip(
								db
										.getDictionary("testmap", UpdateMode.DISALLOW)
										.map(dictionary -> DatabaseMapDictionaryDeep.deepTail(dictionary,
												new FixedStringSerializer(3),
												4,
												new SubStageGetterMap<>(new FixedStringSerializer(4), Serializer.noop())
										)),
								db
										.getDictionary("testmap", UpdateMode.DISALLOW)
										.map(dictionary -> DatabaseMapDictionaryDeep.deepTail(dictionary,
												new FixedStringSerializer(6),
												7,
												new SubStageGetterMap<>(new FixedStringSerializer(7), Serializer.noop())
										))
						)
						.single()
						.flatMap(tuple -> {
							var db1 = tuple.getT1();
							return Flux
									.fromIterable(originalSuperKeys)
									.flatMapSequential(superKey -> db1.at(null, superKey))
									.flatMapSequential(at -> Flux
											.fromIterable(originalSubKeys)
											.flatMapSequential(subKey -> at.at(null, subKey))
											.flatMapSequential(at2 -> at2.set(DUMMY_VALUE))
									)
									.then(db
											.takeSnapshot()
											.map(snapshot -> new CompositeSnapshot(Map.of(CompositeDatabasePartLocation.of(KV_DATABASE,
													db.getDatabaseName()), snapshot)))
									)
									.map(snapshot -> Tuples.of(tuple.getT1(), tuple.getT2(), snapshot))
									.single();
						})
						.single()
						.flatMap(tuple -> tuple.getT1().clear().thenReturn(tuple))
						.flatMap(tuple -> tuple
								.getT1()
								.leavesCount(null, false)
								.flatMap(count -> count == 0 ? Mono.just(tuple) : Mono.error(new IllegalStateException(
										"Failed to clear map. Remaining elements after clear: " + count)))
						)
						.flatMapMany(tuple -> {
							var oldDb = tuple.getT1();
							var newDb = tuple.getT2();
							var snapshot = tuple.getT3();

							return oldDb
									.getAllStages(snapshot)
									.flatMapSequential(parentEntry -> Mono
											.fromCallable(() -> newPrefix + parentEntry.getKey())
											.flatMapMany(newId1 -> parentEntry.getValue()
													.getAllValues(snapshot)
													.flatMapSequential(entry -> Mono
															.fromCallable(() -> newPrefix + entry.getKey())
															.flatMap(newId2 -> newDb
																	.at(null, newId1)
																	.flatMap(newStage -> newStage.putValue(newId2, entry.getValue()))
																	.thenReturn(Map.entry(newId1, newId2))
															)
													)
											)
									)
									.concatWith(db
											.releaseSnapshot(snapshot.getSnapshot(db))
											.then(oldDb.close())
											.then(newDb.close())
											.then(Mono.empty())
									);
						})
				);
	}

	private static class FixedStringSerializer implements SerializerFixedBinaryLength<String, byte[]> {

		private final int size;

		public FixedStringSerializer(int i) {
			this.size = i;
		}

		@Override
		public int getSerializedBinaryLength() {
			return size;
		}

		@Override
		public @NotNull String deserialize(byte @NotNull [] serialized) {
			return new String(serialized, StandardCharsets.US_ASCII);
		}

		@Override
		public byte @NotNull [] serialize(@NotNull String deserialized) {
			return deserialized.getBytes(StandardCharsets.US_ASCII);
		}
	}
}
