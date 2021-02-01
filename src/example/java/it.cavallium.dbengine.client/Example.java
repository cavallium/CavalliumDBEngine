package it.cavallium.dbengine.client;

import com.google.common.primitives.Ints;
import it.cavallium.dbengine.database.Column;
import it.cavallium.dbengine.database.LLKeyValueDatabase;
import it.cavallium.dbengine.database.collections.DatabaseMapDictionary;
import it.cavallium.dbengine.database.collections.DatabaseMapDictionaryDeep;
import it.cavallium.dbengine.database.collections.Serializer;
import it.cavallium.dbengine.database.collections.SerializerFixedBinaryLength;
import it.cavallium.dbengine.database.collections.SubStageGetterSingleBytes;
import it.cavallium.dbengine.database.disk.LLLocalDatabaseConnection;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.DecimalFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.One;
import reactor.core.scheduler.Schedulers;
import reactor.util.function.Tuples;

public class Example {

	private static final boolean printPreviousValue = false;
	private static final int numRepeats = 1000;
	private static final int batchSize = 10000;

	public static void main(String[] args) throws InterruptedException {
		/*
		testAtPut();
		testPutValueAndGetPrevious();
		testPutValue();
		testAtPut()
				.then(rangeTestAtPut())
				.then(testPutValue())
				.then(rangeTestPutValue())
				.then(testPutMulti())
				.then(rangeTestPutMulti())
				.subscribeOn(Schedulers.parallel())
				.blockOptional();


		 */

		rangeTestPutMultiProgressive()
				.then(rangeTestPutMultiSame())
				.subscribeOn(Schedulers.parallel())
				.blockOptional();
	}

	private static Mono<Void> testAtPut() {
		var ssg = new SubStageGetterSingleBytes();
		var ser = SerializerFixedBinaryLength.noop(4);
		var itemKey = new byte[]{0, 1, 2, 3};
		var newValue = new byte[]{4, 5, 6, 7};
		return test("MapDictionaryDeep::at::put (same key, same value, " + batchSize + " times)",
				tempDb()
						.flatMap(db -> db.getDictionary("testmap").map(dict -> Tuples.of(db, dict)))
						.map(tuple -> tuple.mapT2(dict -> DatabaseMapDictionaryDeep.simple(dict, ssg, ser))),
				tuple -> Flux.range(0, batchSize).flatMap(n -> Mono
						.defer(() -> Mono
								.fromRunnable(() -> {
									if (printPreviousValue)
										System.out.println("Setting new value at key " + Arrays.toString(itemKey) + ": " + Arrays.toString(newValue));
								})
								.then(tuple.getT2().at(null, itemKey))
								.flatMap(handle -> handle.setAndGetPrevious(newValue))
								.doOnSuccess(oldValue -> {
									if (printPreviousValue)
										System.out.println("Old value: " + (oldValue == null ? "None" : Arrays.toString(oldValue)));
								})
						))
						.then(),
				numRepeats,
				tuple -> tuple.getT1().close());
	}

	private static Mono<Void> testPutValueAndGetPrevious() {
		var ssg = new SubStageGetterSingleBytes();
		var ser = SerializerFixedBinaryLength.noop(4);
		var itemKey = new byte[]{0, 1, 2, 3};
		var newValue = new byte[]{4, 5, 6, 7};
		return test("MapDictionaryDeep::putValueAndGetPrevious (same key, same value, " + batchSize + " times)",
				tempDb()
						.flatMap(db -> db.getDictionary("testmap").map(dict -> Tuples.of(db, dict)))
						.map(tuple -> tuple.mapT2(dict -> DatabaseMapDictionaryDeep.simple(dict, ssg, ser))),
				tuple -> Flux.range(0, batchSize).flatMap(n -> Mono
						.defer(() -> Mono
								.fromRunnable(() -> {
									if (printPreviousValue)
										System.out.println("Setting new value at key " + Arrays.toString(itemKey) + ": " + Arrays.toString(newValue));
								})
								.then(tuple.getT2().putValueAndGetPrevious(itemKey, newValue))
								.doOnSuccess(oldValue -> {
									if (printPreviousValue)
										System.out.println("Old value: " + (oldValue == null ? "None" : Arrays.toString(oldValue)));
								})
						))
						.then(),
				numRepeats,
				tuple -> tuple.getT1().close());
	}

	private static Mono<Void> testPutValue() {
		var ssg = new SubStageGetterSingleBytes();
		var ser = SerializerFixedBinaryLength.noop(4);
		var itemKey = new byte[]{0, 1, 2, 3};
		var newValue = new byte[]{4, 5, 6, 7};
		return test("MapDictionaryDeep::putValue (same key, same value, " + batchSize + " times)",
				tempDb()
						.flatMap(db -> db.getDictionary("testmap").map(dict -> Tuples.of(db, dict)))
						.map(tuple -> tuple.mapT2(dict -> DatabaseMapDictionaryDeep.simple(dict, ssg, ser))),
				tuple -> Flux.range(0, batchSize).flatMap(n -> Mono
						.defer(() -> Mono
								.fromRunnable(() -> {
									if (printPreviousValue)
										System.out.println("Setting new value at key " + Arrays.toString(itemKey) + ": " + Arrays.toString(newValue));
								})
								.then(tuple.getT2().putValue(itemKey, newValue))
						))
						.then(),
				numRepeats,
				tuple -> tuple.getT1().close());
	}

	private static Mono<Void> testPutMulti() {
		var ssg = new SubStageGetterSingleBytes();
		var ser = SerializerFixedBinaryLength.noop(4);
		HashMap<byte[], byte[]> keysToPut = new HashMap<>();
		for (int i = 0; i < batchSize; i++) {
			keysToPut.put(Ints.toByteArray(i * 3), Ints.toByteArray(i * 11));
		}
		var putMultiFlux = Flux.fromIterable(keysToPut.entrySet());
		return test("MapDictionaryDeep::putMulti (batch of " + batchSize + " entries)",
				tempDb()
						.flatMap(db -> db.getDictionary("testmap").map(dict -> Tuples.of(db, dict)))
						.map(tuple -> tuple.mapT2(dict -> DatabaseMapDictionaryDeep.simple(dict, ssg, ser))),
				tuple -> Mono.defer(() -> tuple.getT2().putMulti(putMultiFlux)),
				numRepeats,
				tuple -> Mono
						.fromRunnable(() -> System.out.println("Calculating size"))
						.then(tuple.getT2().size(null, false))
						.doOnNext(s -> System.out.println("Size after: " + s))
						.then(tuple.getT1().close())
		);
	}

	private static Mono<Void> rangeTestAtPut() {
		var ser = SerializerFixedBinaryLength.noop(4);
		var vser = Serializer.noop();
		var itemKey = new byte[]{0, 1, 2, 3};
		var newValue = new byte[]{4, 5, 6, 7};
		return test("MapDictionary::at::put (same key, same value, " + batchSize + " times)",
				tempDb()
						.flatMap(db -> db.getDictionary("testmap").map(dict -> Tuples.of(db, dict)))
						.map(tuple -> tuple.mapT2(dict -> DatabaseMapDictionary.simple(dict, ser, vser))),
				tuple -> Flux.range(0, batchSize).flatMap(n -> Mono
						.defer(() -> Mono
								.fromRunnable(() -> {
									if (printPreviousValue)
										System.out.println("Setting new value at key " + Arrays.toString(itemKey) + ": " + Arrays.toString(newValue));
								})
								.then(tuple.getT2().at(null, itemKey))
								.flatMap(handle -> handle.setAndGetPrevious(newValue))
								.doOnSuccess(oldValue -> {
									if (printPreviousValue)
										System.out.println("Old value: " + (oldValue == null ? "None" : Arrays.toString(oldValue)));
								})
						))
						.then(),
				numRepeats,
				tuple -> tuple.getT1().close());
	}

	private static Mono<Void> rangeTestPutValueAndGetPrevious() {
		var ser = SerializerFixedBinaryLength.noop(4);
		var vser = Serializer.noop();
		var itemKey = new byte[]{0, 1, 2, 3};
		var newValue = new byte[]{4, 5, 6, 7};
		return test("MapDictionary::putValueAndGetPrevious (same key, same value, " + batchSize + " times)",
				tempDb()
						.flatMap(db -> db.getDictionary("testmap").map(dict -> Tuples.of(db, dict)))
						.map(tuple -> tuple.mapT2(dict -> DatabaseMapDictionary.simple(dict, ser, vser))),
				tuple -> Flux.range(0, batchSize).flatMap(n -> Mono
						.defer(() -> Mono
								.fromRunnable(() -> {
									if (printPreviousValue)
										System.out.println("Setting new value at key " + Arrays.toString(itemKey) + ": " + Arrays.toString(newValue));
								})
								.then(tuple.getT2().putValueAndGetPrevious(itemKey, newValue))
								.doOnSuccess(oldValue -> {
									if (printPreviousValue)
										System.out.println("Old value: " + (oldValue == null ? "None" : Arrays.toString(oldValue)));
								})
						))
						.then(),
				numRepeats,
				tuple -> tuple.getT1().close());
	}

	private static Mono<Void> rangeTestPutValue() {
		var ser = SerializerFixedBinaryLength.noop(4);
		var vser = Serializer.noop();
		var itemKey = new byte[]{0, 1, 2, 3};
		var newValue = new byte[]{4, 5, 6, 7};
		return test("MapDictionary::putValue (same key, same value, " + batchSize + " times)",
				tempDb()
						.flatMap(db -> db.getDictionary("testmap").map(dict -> Tuples.of(db, dict)))
						.map(tuple -> tuple.mapT2(dict -> DatabaseMapDictionary.simple(dict, ser, vser))),
				tuple -> Flux.range(0, batchSize).flatMap(n -> Mono
						.defer(() -> Mono
								.fromRunnable(() -> {
									if (printPreviousValue)
										System.out.println("Setting new value at key " + Arrays.toString(itemKey) + ": " + Arrays.toString(newValue));
								})
								.then(tuple.getT2().putValue(itemKey, newValue))
						))
						.then(),
				numRepeats,
				tuple -> tuple.getT1().close());
	}

	private static Mono<Void> rangeTestPutMultiSame() {
		var ser = SerializerFixedBinaryLength.noop(4);
		var vser = Serializer.noop();
		HashMap<byte[], byte[]> keysToPut = new HashMap<>();
		for (int i = 0; i < batchSize; i++) {
			keysToPut.put(Ints.toByteArray(i * 3), Ints.toByteArray(i * 11));
		}
		return test("MapDictionary::putMulti (batch of " + batchSize + " entries)",
				tempDb()
						.flatMap(db -> db.getDictionary("testmap").map(dict -> Tuples.of(db, dict)))
						.map(tuple -> tuple.mapT2(dict -> DatabaseMapDictionary.simple(dict, ser, vser))),
				tuple -> Mono
						.defer(() -> tuple.getT2().putMulti(Flux.fromIterable(keysToPut.entrySet()))
						),
				numRepeats,
				tuple -> Mono
						.fromRunnable(() -> System.out.println("Calculating size"))
						.then(tuple.getT2().size(null, false))
						.doOnNext(s -> System.out.println("Size after: " + s))
						.then(tuple.getT1().close())
		);
	}

	private static Mono<Void> rangeTestPutMultiProgressive() {
		var ser = SerializerFixedBinaryLength.noop(4);
		var vser = Serializer.noop();
		AtomicInteger ai = new AtomicInteger(0);
		return test("MapDictionary::putMulti (batch of " + batchSize + " entries)",
				tempDb()
						.flatMap(db -> db.getDictionary("testmap").map(dict -> Tuples.of(db, dict)))
						.map(tuple -> tuple.mapT2(dict -> DatabaseMapDictionary.simple(dict, ser, vser))),
				tuple -> Mono
						.defer(() -> {
							var aiv = ai.incrementAndGet();
							HashMap<byte[], byte[]> keysToPut = new HashMap<>();
							for (int i = 0; i < batchSize; i++) {
								keysToPut.put(
										Ints.toByteArray(i * 3 + (batchSize * aiv)),
										Ints.toByteArray(i * 11 + (batchSize * aiv))
								);
							}
							return tuple.getT2().putMulti(Flux.fromIterable(keysToPut.entrySet()));
						}),
				numRepeats,
				tuple -> Mono
						.fromRunnable(() -> System.out.println("Calculating size"))
						.then(tuple.getT2().size(null, false))
						.doOnNext(s -> System.out.println("Size after: " + s))
						.then(tuple.getT1().close())
		);
	}

	private static <U> Mono<? extends LLKeyValueDatabase> tempDb() {
		var wrkspcPath = Path.of("/tmp/tempdb/");
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

	public static <A, B, C> Mono<Void> test(String name,
			Mono<A> setup,
			Function<A, Mono<B>> test,
			long numRepeats,
			Function<A, Mono<C>> close) {
		One<Instant> instantInit = Sinks.one();
		One<Instant> instantInitTest = Sinks.one();
		One<Instant> instantEndTest = Sinks.one();
		One<Instant> instantEnd = Sinks.one();
		Duration WAIT_TIME = Duration.ofSeconds(5);
		Duration WAIT_TIME_END = Duration.ofSeconds(5);
		return Mono
				.delay(WAIT_TIME)
				.doOnSuccess(s -> {
					System.out.println("----------------------------------------------------------------------");
					System.out.println(name);
				})
				.then(Mono.fromRunnable(() -> instantInit.tryEmitValue(now())))
				.then(setup)
				.doOnSuccess(s -> instantInitTest.tryEmitValue(now()))
				.flatMap(a -> Mono.defer(() -> test.apply(a)).repeat(numRepeats - 1)
						.then()
						.doOnSuccess(s -> instantEndTest.tryEmitValue(now()))
						.then(close.apply(a)))
				.doOnSuccess(s -> instantEnd.tryEmitValue(now()))
				.then(Mono.zip(instantInit.asMono(), instantInitTest.asMono(), instantEndTest.asMono(), instantEnd.asMono()))
				.doOnSuccess(tuple -> {
					System.out.println(
							"\t - Executed " + DecimalFormat.getInstance(Locale.ITALY).format((numRepeats * batchSize)) + " times:");
					System.out.println("\t - Test time: " + DecimalFormat
							.getInstance(Locale.ITALY)
							.format(Duration.between(tuple.getT2(), tuple.getT3()).toNanos() / (double) (numRepeats * batchSize) / (double) 1000000)
							+ "ms");
					System.out.println("\t - Test speed: " + DecimalFormat
							.getInstance(Locale.ITALY)
							.format((numRepeats * batchSize) / (Duration.between(tuple.getT2(), tuple.getT3()).toNanos() / (double) 1000000 / (double) 1000))
							+ " tests/s");
					System.out.println("\t - Total time: " + DecimalFormat
							.getInstance(Locale.ITALY)
							.format(Duration.between(tuple.getT2(), tuple.getT3()).toNanos() / (double) 1000000) + "ms");
					System.out.println("\t - Total time (setup+test+end): " + DecimalFormat
							.getInstance(Locale.ITALY)
							.format(Duration.between(tuple.getT1(), tuple.getT4()).toNanos() / (double) 1000000) + "ms");
					System.out.println("----------------------------------------------------------------------");
				})
				.delayElement(WAIT_TIME_END)
				.then();
	}

	public static Instant now() {
		return Instant.ofEpochSecond(0, System.nanoTime());
	}
}