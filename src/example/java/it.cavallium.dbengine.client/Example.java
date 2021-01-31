package it.cavallium.dbengine.client;

import io.netty.buffer.Unpooled;
import it.cavallium.dbengine.database.Column;
import it.cavallium.dbengine.database.LLKeyValueDatabase;
import it.cavallium.dbengine.database.collections.DatabaseMapDictionary;
import it.cavallium.dbengine.database.collections.FixedLengthSerializer;
import it.cavallium.dbengine.database.collections.SubStageGetterSingleBytes;
import it.cavallium.dbengine.database.disk.LLLocalDatabaseConnection;
import java.nio.file.Path;
import java.text.DecimalFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.One;
import reactor.core.scheduler.Schedulers;
import reactor.util.function.Tuples;

public class Example {

	private static final boolean printPreviousValue = false;

	public static void main(String[] args) {
		System.out.println("Test");
		testAtPut();
		testPutValueAndGetPrevious();
		testPutValue()
				.subscribeOn(Schedulers.parallel())
				.blockOptional();
	}

	private static Mono<Void> testAtPut() {
		var ssg = new SubStageGetterSingleBytes();
		var ser = FixedLengthSerializer.noop(4);
		var itemKey = new byte[]{0, 1, 2, 3};
		var newValue = new byte[]{4, 5, 6, 7};
		var itemKeyBuffer = Unpooled.wrappedBuffer(itemKey);
		return test("MapDictionary::at::put (same key, same value)",
				tempDb()
						.flatMap(db -> db.getDictionary("testmap").map(dict -> Tuples.of(db, dict)))
						.map(tuple -> tuple.mapT2(dict -> DatabaseMapDictionary.simple(dict, ssg, ser))),
				tuple -> Mono
						.defer(() -> Mono
								.fromRunnable(() -> {
									if (printPreviousValue)
										System.out.println("Setting new value at key " + Arrays.toString(itemKey) + ": " + Arrays.toString(newValue));
								})
								.then(tuple.getT2().at(null, itemKeyBuffer))
								.flatMap(handle -> handle.setAndGetPrevious(newValue))
								.doOnSuccess(oldValue -> {
									if (printPreviousValue)
										System.out.println("Old value: " + (oldValue == null ? "None" : Arrays.toString(oldValue)));
								})
						),
				100000,
				tuple -> tuple.getT1().close());
	}

	private static Mono<Void> testPutValueAndGetPrevious() {
		var ssg = new SubStageGetterSingleBytes();
		var ser = FixedLengthSerializer.noop(4);
		var itemKey = new byte[]{0, 1, 2, 3};
		var newValue = new byte[]{4, 5, 6, 7};
		var itemKeyBuffer = Unpooled.wrappedBuffer(itemKey);
		return test("MapDictionary::putValueAndGetPrevious (same key, same value)",
				tempDb()
						.flatMap(db -> db.getDictionary("testmap").map(dict -> Tuples.of(db, dict)))
						.map(tuple -> tuple.mapT2(dict -> DatabaseMapDictionary.simple(dict, ssg, ser))),
				tuple -> Mono
						.defer(() -> Mono
								.fromRunnable(() -> {
									if (printPreviousValue)
										System.out.println("Setting new value at key " + Arrays.toString(itemKey) + ": " + Arrays.toString(newValue));
								})
								.then(tuple.getT2().putValueAndGetPrevious(itemKeyBuffer, newValue))
								.doOnSuccess(oldValue -> {
									if (printPreviousValue)
										System.out.println("Old value: " + (oldValue == null ? "None" : Arrays.toString(oldValue)));
								})
						),
				10000,
				tuple -> tuple.getT1().close());
	}

	private static Mono<Void> testPutValue() {
		var ssg = new SubStageGetterSingleBytes();
		var ser = FixedLengthSerializer.noop(4);
		var itemKey = new byte[]{0, 1, 2, 3};
		var newValue = new byte[]{4, 5, 6, 7};
		var itemKeyBuffer = Unpooled.wrappedBuffer(itemKey);
		return test("MapDictionary::putValue (same key, same value)",
				tempDb()
						.flatMap(db -> db.getDictionary("testmap").map(dict -> Tuples.of(db, dict)))
						.map(tuple -> tuple.mapT2(dict -> DatabaseMapDictionary.simple(dict, ssg, ser))),
				tuple -> Mono
						.defer(() -> Mono
								.fromRunnable(() -> {
									if (printPreviousValue)
										System.out.println("Setting new value at key " + Arrays.toString(itemKey) + ": " + Arrays.toString(newValue));
								})
								.then(tuple.getT2().putValue(itemKeyBuffer, newValue))
						),
				10000,
				tuple -> tuple.getT1().close());
	}

	private static <U> Mono<? extends LLKeyValueDatabase> tempDb() {
		return new LLLocalDatabaseConnection(Path.of("/tmp/"), true)
				.connect()
				.flatMap(conn -> conn.getDatabase("testdb", List.of(Column.dictionary("testmap")), false));
	}

	public static  <A, B, C> Mono<Void> test(String name, Mono<A> setup, Function<A, Mono<B>> test, long numRepeats, Function<A, Mono<C>> close) {
		One<Instant> instantInit = Sinks.one();
		One<Instant> instantInitTest = Sinks.one();
		One<Instant> instantEndTest = Sinks.one();
		One<Instant> instantEnd = Sinks.one();
		return Mono
				.fromRunnable(() -> instantInit.tryEmitValue(now()))
				.then(setup)
				.doOnSuccess(s -> instantInitTest.tryEmitValue(now()))
				.flatMap(a -> Mono.defer(() -> test.apply(a))
						.repeat(numRepeats)
						.then()
						.doOnSuccess(s -> instantEndTest.tryEmitValue(now()))
						.then(close.apply(a)))
				.doOnSuccess(s -> instantEnd.tryEmitValue(now()))
				.then(Mono.zip(instantInit.asMono(), instantInitTest.asMono(), instantEndTest.asMono(), instantEnd.asMono()))
				.doOnSuccess(tuple -> {
					System.out.println("----------------------------------------------------------------------");
					System.out.println(name);
					System.out.println(
							"\t - Executed " + DecimalFormat.getInstance(Locale.ITALY).format(numRepeats) + " times:");
					System.out.println("\t - Test time: " + DecimalFormat
							.getInstance(Locale.ITALY)
							.format(Duration.between(tuple.getT2(), tuple.getT3()).toNanos() / (double) numRepeats / (double) 1000000)
							+ "ms");
					System.out.println("\t - Test speed: " + DecimalFormat
							.getInstance(Locale.ITALY)
							.format(numRepeats / (Duration.between(tuple.getT2(), tuple.getT3()).toNanos() / (double) 1000000 / (double) 1000))
							+ " tests/s");
					System.out.println("\t - Total time: " + DecimalFormat
							.getInstance(Locale.ITALY)
							.format(Duration.between(tuple.getT2(), tuple.getT3()).toNanos() / (double) 1000000) + "ms");
					System.out.println("\t - Total time (setup+test+end): " + DecimalFormat
							.getInstance(Locale.ITALY)
							.format(Duration.between(tuple.getT1(), tuple.getT4()).toNanos() / (double) 1000000) + "ms");
					System.out.println("----------------------------------------------------------------------");
				})
				.then();
	}

	public static Instant now() {
		return Instant.ofEpochSecond(0, System.nanoTime());
	}
}