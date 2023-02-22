package it.cavallium.dbengine.tests;

import static it.cavallium.dbengine.tests.DbTestUtils.BIG_STRING;
import static it.cavallium.dbengine.tests.DbTestUtils.ensureNoLeaks;
import static it.cavallium.dbengine.tests.DbTestUtils.isCIMode;
import static it.cavallium.dbengine.tests.DbTestUtils.run;
import static it.cavallium.dbengine.tests.DbTestUtils.runVoid;
import static it.cavallium.dbengine.tests.DbTestUtils.tempDatabaseMapDictionaryDeepMap;
import static it.cavallium.dbengine.tests.DbTestUtils.tempDictionary;

import com.google.common.collect.Streams;
import it.cavallium.dbengine.database.UpdateMode;
import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectSortedMap;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@TestMethodOrder(MethodOrderer.MethodName.class)
public abstract class TestDictionaryMapDeep {

	private final Logger log = LogManager.getLogger(this.getClass());
	private boolean checkLeaks = true;

	private static boolean isTestBadKeysEnabled() {
		return !isCIMode() && System.getProperty("badkeys", "true").equalsIgnoreCase("true");
	}

	protected abstract TemporaryDbGenerator getTempDbGenerator();

	record Tuple2<X, Y>(X getT1, Y getT2) {}
	record Tuple3<X, Y, Z>(X getT1, Y getT2, Z getT3) {}
	record Tuple4<X, Y, Z, W>(X getT1, Y getT2, Z getT3, W getT4) {}
	record Tuple5<X, Y, Z, W, X1>(X getT1, Y getT2, Z getT3, W getT4, X1 getT5) {}

	private static Stream<Arguments> provideArgumentsSet() {
		var goodKeys = Set.of("12345");
		Set<String> badKeys;
		if (isTestBadKeysEnabled()) {
			badKeys = Set.of("", "aaaa", "aaaaaa");
		} else {
			badKeys = Set.of();
		}
		Set<Tuple2<String, Boolean>> keys = Stream.concat(
				goodKeys.stream().map(s -> new Tuple2<>(s, false)),
				badKeys.stream().map(s -> new Tuple2<>(s, true))
		).collect(Collectors.toSet());
		var values = Set.of(
				new Object2ObjectLinkedOpenHashMap<>(Map.of("123456", "a", "234567", "")),
				new Object2ObjectLinkedOpenHashMap<>(Map.of("123456", "\0", "234567", "\0\0", "345678", BIG_STRING))
		);

		return keys
				.stream()
				.flatMap(keyTuple -> {
					Stream<Object2ObjectLinkedOpenHashMap<String, String>> strm;
					if (keyTuple.getT2()) {
						strm = values.stream().limit(1);
					} else {
						strm = values.stream();
					}
					return strm.map(val -> new Tuple3<>(keyTuple.getT1(), val, keyTuple.getT2()));
				})
				.flatMap(entryTuple -> Arrays.stream(UpdateMode.values()).map(updateMode -> new Tuple4<>(updateMode,
						entryTuple.getT1(),
						entryTuple.getT2(),
						entryTuple.getT3()
				)))
				.map(fullTuple -> Arguments.of(fullTuple.getT1(), fullTuple.getT2(), fullTuple.getT3(), fullTuple.getT4()));
	}

	private static Stream<Arguments> provideArgumentsPut() {
		var goodKeys1 = isCIMode() ? List.of("12345") : List.of("12345", "zebra");
		List<String> badKeys1;
		if (isTestBadKeysEnabled()) {
			badKeys1 = List.of("", "a", "aaaa", "aaaaaa");
		} else {
			badKeys1 = List.of();
		}
		var goodKeys2 = isCIMode() ? List.of("123456") : List.of("123456", "anatra");
		List<String> badKeys2;
		if (isTestBadKeysEnabled()) {
			badKeys2 = List.of("", "a", "aaaaa", "aaaaaaa");
		} else {
			badKeys2 = List.of();
		}

		var values = isCIMode() ? List.of("val") : List.of("a", "", "\0", "\0\0", "z", "azzszgzczqz", BIG_STRING);

		Stream<Tuple4<String, String, String, Boolean>> failOnKeys1 = badKeys1.stream()
				.map(badKey1 -> new Tuple4<>(
						badKey1,
						goodKeys2.stream().findFirst().orElseThrow(),
						values.stream().findFirst().orElseThrow(),
						true
				));
		Stream<Tuple4<String, String, String, Boolean>> failOnKeys2 = badKeys2.stream()
				.map(badKey2 -> new Tuple4<>(
						goodKeys1.stream().findFirst().orElseThrow(),
						badKey2,
						values.stream().findFirst().orElseThrow(),
						true
				));

		Stream<Tuple4<String, String, String, Boolean>> goodKeys1And2 = values.stream()
				.map(value -> new Tuple4<>(
						goodKeys1.stream().findFirst().orElseThrow(),
						goodKeys2.stream().findFirst().orElseThrow(),
						value,
						false
				));

		Stream<Tuple4<String, String, String, Boolean>> keys1And2 = Streams.concat(
						goodKeys1And2,
						failOnKeys1,
						failOnKeys2
				);

		return keys1And2
				.flatMap(entryTuple -> Stream.of(UpdateMode.values())
						.map(updateMode -> new Tuple5<>(updateMode,
								entryTuple.getT1(),
								entryTuple.getT2(),
								entryTuple.getT3(),
								entryTuple.getT4()
						))
				)
				.map(fullTuple -> Arguments.of(fullTuple.getT1(),
						fullTuple.getT2(),
						fullTuple.getT3(),
						fullTuple.getT4(),
						fullTuple.getT5()
				))
				.sequential();
	}

	@BeforeEach
	public void beforeEach() {
		ensureNoLeaks(false, false);
	}

	@AfterEach
	public void afterEach() {
		if (!isCIMode() && checkLeaks) {
			ensureNoLeaks(true, false);
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsSet")
	public void testPutValue(UpdateMode updateMode,
			String key,
			Object2ObjectSortedMap<String, String> value,
			boolean shouldFail) throws IOException {
		var gen = getTempDbGenerator();
		var db = gen.openTempDb();
		var dict = tempDictionary(db.db(), updateMode);
		var map = tempDatabaseMapDictionaryDeepMap(dict, 5, 6);

		log.debug("Put \"{}\" = \"{}\"", key, value);
		runVoid(shouldFail, () -> map.putValue(key, value));

		var resultingMapSize = map.leavesCount(null, false);
		Assertions.assertEquals(shouldFail ? 0 : value.size(), resultingMapSize);

		var resultingMap = map.get(null);
		Assertions.assertEquals(shouldFail ? null : Map.of(key, value), resultingMap);

		gen.closeTempDb(db);
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsSet")
	public void testGetValue(UpdateMode updateMode,
			String key,
			Object2ObjectSortedMap<String, String> value,
			boolean shouldFail) throws IOException {
		var gen = getTempDbGenerator();
		var db = gen.openTempDb();
		var dict = tempDictionary(db.db(), updateMode);
		var map = tempDatabaseMapDictionaryDeepMap(dict, 5, 6);

		log.debug("Put \"{}\" = \"{}\"", key, value);
		runVoid(shouldFail, () -> map.putValue(key, value));

		log.debug("Get \"{}\"", key);
		var returnedValue = run(shouldFail, () -> map.getValue(null, key));

		Assertions.assertEquals(shouldFail ? null : value, returnedValue);

		gen.closeTempDb(db);
	}
/*
	@ParameterizedTest
	@MethodSource("provideArgumentsSet")
	public void testSetValueGetAllValues(UpdateMode updateMode, String key, Object2ObjectSortedMap<String, String> value,
			boolean shouldFail) {
		var stpVer = StepVerifier
				.create(tempDb(getTempDbGenerator(), allocator, db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryDeepMap(dict, 5, 6))
						.flatMapMany(map -> map
								.putValue(key, value)
								.thenMany(map.getAllValues(null, false))
								.doFinally(s -> map.close())
						)
				));
		if (shouldFail) {
			this.checkLeaks = false;
			stpVer.verifyError();
		} else {
			stpVer.expectNext(Map.entry(key, value)).verifyComplete();
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsSet")
	public void testAtSetGetAllStagesGetAllValues(UpdateMode updateMode,
			String key,
			Object2ObjectSortedMap<String, String> value,
			boolean shouldFail) {
		var remainingEntries = new ConcurrentHashMap<Tuple3<String, String, String>, Boolean>().keySet(true);
		Step<Tuple3<String, String, String>> stpVer = StepVerifier
				.create(tempDb(getTempDbGenerator(), allocator, db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryDeepMap(dict, 5, 6))
						.flatMapMany(map_ -> Flux.using(
								() -> map_,
								map -> map
								.at(null, key)
								.flatMap(v_ -> Mono.using(
										() -> v_,
										v -> v.set(value),
										SimpleResource::close
								))
								.then(map
										.at(null, "capra")
										.flatMap(v_ -> Mono.using(
												() -> v_,
												v -> v.set(new Object2ObjectLinkedOpenHashMap<>(Map.of("normal", "123", "ormaln", "456"))),
												SimpleResource::close
										))
								)
								.thenMany(map
										.getAllStages(null, false)
										.flatMap(v -> v.getValue()
												.getAllValues(null, false)
												.map(result -> new Tuple2<>(v.getKey(), result.getKey(), result.getValue()))
												.doFinally(s -> v.getValue().close())
										)
								),
								SimpleResource::close
						))
				));
		if (shouldFail) {
			this.checkLeaks = false;
			stpVer.verifyError();
		} else {
			value.forEach((k, v) -> remainingEntries.add(new Tuple2<>(key, k, v)));
			remainingEntries.add(new Tuple2<>("capra", "normal", "123"));
			remainingEntries.add(new Tuple2<>("capra", "ormaln", "456"));
			for (Tuple3<String, String, String> ignored : remainingEntries) {
				stpVer = stpVer.expectNextMatches(remainingEntries::remove);
			}
			stpVer.verifyComplete();
			assert remainingEntries.isEmpty();
		}
	}

	@ParameterizedTest
	@MethodSource({"provideArgumentsPut"})
	public void testAtPutValueAtGetValue(UpdateMode updateMode, String key1, String key2, String value,
			boolean shouldFail) {
		var stpVer = StepVerifier
				.create(tempDb(getTempDbGenerator(), allocator, db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryDeepMap(dict, 5, 6))
						.flatMap(map -> map
								.at(null, key1)
								.flatMap(v -> v
										.putValue(key2, value)
										.doFinally(s -> v.close())
								)
								.then(map
										.at(null, key1)
										.flatMap(v -> v
												.getValue(null, key2)
												.doFinally(s -> v.close())
										)
								)
								.doFinally(s -> map.close())
						)
				));
		if (shouldFail) {
			this.checkLeaks = false;
			stpVer.verifyError();
		} else {
			stpVer.expectNext(value).verifyComplete();
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsSet")
	public void testSetAndGetPrevious(UpdateMode updateMode,
			String key,
			Object2ObjectSortedMap<String, String> value,
			boolean shouldFail) {
		var stpVer = StepVerifier
				.create(tempDb(getTempDbGenerator(), allocator, db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryDeepMap(dict, 5, 6))
						.flatMapMany(map -> Flux
								.concat
										(map
												.putValueAndGetPrevious(key, new Object2ObjectLinkedOpenHashMap<>(Map.of("error?", "error.")))
												.defaultIfEmpty(new Object2ObjectLinkedOpenHashMap<>(Map.of("nothing", "nothing"))),
										map.putValueAndGetPrevious(key, value),
										map.putValueAndGetPrevious(key, value)
								)
								.doFinally(s -> map.close())
						)
				));
		if (shouldFail) {
			this.checkLeaks = false;
			stpVer.verifyError();
		} else {
			stpVer
					.expectNext(new Object2ObjectLinkedOpenHashMap<>(Map.of("nothing", "nothing")),
							new Object2ObjectLinkedOpenHashMap<>(Map.of("error?", "error."))
					)
					.expectNext(value)
					.verifyComplete();
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsPut")
	public void testAtPutValueAndGetPrevious(UpdateMode updateMode, String key1, String key2, String value,
			boolean shouldFail) {
		var stpVer = StepVerifier
				.create(tempDb(getTempDbGenerator(), allocator, db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryDeepMap(dict, 5, 6))
						.flatMapMany(map -> Flux
								.concat(
										map
												.at(null, key1)
												.flatMap(v -> v
														.putValueAndGetPrevious(key2, "error?")
														.doFinally(s -> v.close())
												),
										map
												.at(null, key1)
												.flatMap(v -> v
														.putValueAndGetPrevious(key2, value)
														.doFinally(s -> v.close())
												),
										map
												.at(null, key1)
												.flatMap(v -> v
														.putValueAndGetPrevious(key2, value)
														.doFinally(s -> v.close())
												)
								)
								.doFinally(s -> map.close())
						)
				));
		if (shouldFail) {
			this.checkLeaks = false;
			stpVer.verifyError();
		} else {
			stpVer.expectNext("error?", value).verifyComplete();
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsSet")
	public void testSetValueRemoveAndGetPrevious(UpdateMode updateMode,
			String key,
			Object2ObjectSortedMap<String, String> value,
			boolean shouldFail) {
		var stpVer = StepVerifier
				.create(tempDb(getTempDbGenerator(), allocator, db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryDeepMap(dict, 5, 6))
						.flatMapMany(map -> Flux
								.concat(
										map.removeAndGetPrevious(key),
										map.putValue(key, value).then(map.removeAndGetPrevious(key)),
										map.removeAndGetPrevious(key)
								)
								.doFinally(s -> map.close())
						)
				));
		if (shouldFail) {
			this.checkLeaks = false;
			stpVer.verifyError();
		} else {
			stpVer.expectNext(value).verifyComplete();
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsPut")
	public void testAtPutValueRemoveAndGetPrevious(UpdateMode updateMode, String key1, String key2, String value,
			boolean shouldFail) {
		var stpVer = StepVerifier
				.create(tempDb(getTempDbGenerator(), allocator, db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryDeepMap(dict, 5, 6))
						.flatMapMany(map -> Flux
								.concat(
										map
												.at(null, key1)
												.flatMap(v -> v
														.putValue(key2, "error?")
														.then(v.removeAndGetPrevious(key2))
														.doFinally(s -> v.close())
												),
										map
												.at(null, key1)
												.flatMap(v -> v
														.putValue(key2, value)
														.then(v.removeAndGetPrevious(key2))
														.doFinally(s -> v.close())
												),
										map
												.at(null, key1)
												.flatMap(v -> v.removeAndGetPrevious(key2)
														.doFinally(s -> v.close())
												)
								)
								.doFinally(s -> map.close())
						)
				));
		if (shouldFail) {
			this.checkLeaks = false;
			stpVer.verifyError();
		} else {
			stpVer.expectNext("error?", value).verifyComplete();
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsSet")
	public void testSetValueRemoveAndGetStatus(UpdateMode updateMode,
			String key,
			Object2ObjectSortedMap<String, String> value,
			boolean shouldFail) {
		var stpVer = StepVerifier
				.create(tempDb(getTempDbGenerator(), allocator, db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryDeepMap(dict, 5, 6))
						.flatMapMany(map -> Flux
								.concat(
										map.removeAndGetStatus(key),
										map.putValue(key, value).then(map.removeAndGetStatus(key)),
										map.removeAndGetStatus(key)
								)
								.doFinally(s -> map.close())
						)
				));
		if (shouldFail) {
			this.checkLeaks = false;
			stpVer.verifyError();
		} else {
			stpVer.expectNext(false, true, false).verifyComplete();
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsPut")
	public void testAtPutValueRemoveAndGetStatus(UpdateMode updateMode, String key1, String key2, String value,
			boolean shouldFail) {
		var stpVer = StepVerifier
				.create(tempDb(getTempDbGenerator(), allocator, db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryDeepMap(dict, 5, 6))
						.flatMapMany(map -> Flux
								.concat(
										map
												.at(null, key1)
												.flatMap(v -> v
														.putValue(key2, "error?")
														.then(v.removeAndGetStatus(key2))
														.doFinally(s -> v.close())
												),
										map
												.at(null, key1)
												.flatMap(v -> v
														.putValue(key2, value)
														.then(v.removeAndGetStatus(key2))
														.doFinally(s -> v.close())
												),
										map
												.at(null, key1)
												.flatMap(v -> v.removeAndGetStatus(key2)
														.doFinally(s -> v.close())
												)
								)
								.doFinally(s -> map.close())
						)
				));
		if (shouldFail) {
			this.checkLeaks = false;
			stpVer.verifyError();
		} else {
			stpVer.expectNext(true, true, false).verifyComplete();
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsSet")
	public void testUpdate(UpdateMode updateMode,
			String key,
			Object2ObjectSortedMap<String, String> value,
			boolean shouldFail) {
		if (updateMode != UpdateMode.ALLOW_UNSAFE && !isTestBadKeysEnabled()) {
			return;
		}
		var stpVer = StepVerifier
				.create(tempDb(getTempDbGenerator(), allocator, db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryDeepMap(dict, 5, 6))
						.flatMapMany(map -> Flux
								.concat(
										map.updateValue(key, old -> {
											assert old == null;
											return new Object2ObjectLinkedOpenHashMap<>(Map.of("error?", "error."));
										}),
										map.updateValue(key, old -> {
											assert Objects.equals(old, Map.of("error?", "error."));
											return new Object2ObjectLinkedOpenHashMap<>(Map.of("error?", "error."));
										}),
										map.updateValue(key, old -> {
											assert Objects.equals(old, Map.of("error?", "error."));
											return new Object2ObjectLinkedOpenHashMap<>(Map.of("error?", "error."));
										}),
										map.updateValue(key, old -> {
											assert Objects.equals(old, Map.of("error?", "error."));
											return value;
										}),
										map.updateValue(key, old -> {
											assert Objects.equals(old, value);
											return value;
										})
								)
								.doFinally(s -> map.close())
						)
				));
		if (updateMode != UpdateMode.ALLOW_UNSAFE || shouldFail) {
			stpVer.verifyError();
		} else {
			stpVer.expectNext(true, false, false, true, false).verifyComplete();
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsPut")
	public void testAtUpdate(UpdateMode updateMode, String key1, String key2, String value, boolean shouldFail) {
		if (updateMode == UpdateMode.DISALLOW && !isTestBadKeysEnabled()) {
			return;
		}
		var stpVer = StepVerifier
				.create(tempDb(getTempDbGenerator(), allocator, db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryDeepMap(dict, 5, 6))
						.flatMapMany(map -> Flux
								.concat(
										map
												.at(null, key1)
												.flatMap(v -> v
														.updateValue(key2, prev -> prev)
														.doFinally(s -> v.close())
												),
										map
												.at(null, key1)
												.flatMap(v -> v
														.updateValue(key2, prev -> value)
														.doFinally(s -> v.close())
												),
										map
												.at(null, key1)
												.flatMap(v -> v
														.updateValue(key2, prev -> value)
														.doFinally(s -> v.close())
												),
										map
												.at(null, key1)
												.flatMap(v -> v
														.updateValue(key2, prev -> null)
														.doFinally(s -> v.close())
												)
								)
								.doFinally(s -> map.close())
						)
				));
		if (updateMode == UpdateMode.DISALLOW || shouldFail) {
			stpVer.verifyError();
		} else {
			stpVer.expectNext(false, true, false, true).verifyComplete();
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsSet")
	public void testUpdateGet(UpdateMode updateMode,
			String key,
			Object2ObjectSortedMap<String, String> value,
			boolean shouldFail) {
		if (updateMode != UpdateMode.ALLOW_UNSAFE && !isTestBadKeysEnabled()) {
			return;
		}
		var stpVer = StepVerifier.create(tempDb(getTempDbGenerator(), allocator, db -> tempDictionary(db, updateMode)
				.map(dict -> tempDatabaseMapDictionaryDeepMap(dict, 5, 6))
				.flatMapMany(map -> Flux.concat(
						map.updateValue(key, old -> {
							assert old == null;
							return new Object2ObjectLinkedOpenHashMap<>(Map.of("error?", "error."));
						}).then(map.getValue(null, key)),
						map.updateValue(key, old -> {
							assert Objects.equals(old, Map.of("error?", "error."));
							return new Object2ObjectLinkedOpenHashMap<>(Map.of("error?", "error."));
						}).then(map.getValue(null, key)),
						map.updateValue(key, old -> {
							assert Objects.equals(old, Map.of("error?", "error."));
							return new Object2ObjectLinkedOpenHashMap<>(Map.of("error?", "error."));
						}).then(map.getValue(null, key)),
						map.updateValue(key, old -> {
							assert Objects.equals(old, new Object2ObjectLinkedOpenHashMap<>(Map.of("error?", "error.")));
							return value;
						}).then(map.getValue(null, key)),
						map.updateValue(key, old -> {
							assert Objects.equals(old, value);
							return value;
						}).then(map.getValue(null, key))
				).doFinally(s -> map.close()))
		));
		if (updateMode != UpdateMode.ALLOW_UNSAFE || shouldFail) {
			stpVer.verifyError();
		} else {
			stpVer
					.expectNext(new Object2ObjectLinkedOpenHashMap<>(Map.of("error?", "error.")),
							new Object2ObjectLinkedOpenHashMap<>(Map.of("error?", "error.")),
							new Object2ObjectLinkedOpenHashMap<>(Map.of("error?", "error.")),
							value,
							value
					)
					.verifyComplete();
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsPut")
	public void testAtUpdateGetValue(UpdateMode updateMode, String key1, String key2, String value, boolean shouldFail) {
		if (updateMode == UpdateMode.DISALLOW && !isTestBadKeysEnabled()) {
			return;
		}
		var stpVer = StepVerifier
				.create(tempDb(getTempDbGenerator(), allocator, db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryDeepMap(dict, 5, 6))
						.flatMapMany(map -> Flux
								.concat(
										map
												.at(null, key1)
												.flatMap(v -> v
														.updateValue(key2, prev -> prev)
														.then(v.getValue(null, key2))
														.defaultIfEmpty("empty")
														.doFinally(s -> v.close())
												),
										map
												.at(null, key1)
												.flatMap(v -> v
														.updateValue(key2, prev -> value)
														.then(v.getValue(null, key2))
														.defaultIfEmpty("empty")
														.doFinally(s -> v.close())
												),
										map
												.at(null, key1)
												.flatMap(v -> v
														.updateValue(key2, prev -> value)
														.then(v.getValue(null, key2))
														.defaultIfEmpty("empty")
														.doFinally(s -> v.close())
												),
										map
												.at(null, key1)
												.flatMap(v -> v
														.updateValue(key2, prev -> null)
														.then(v.getValue(null, key2))
														.defaultIfEmpty("empty")
														.doFinally(s -> v.close())
												)
								)
								.doFinally(s -> map.close())
						)
				));
		if (updateMode == UpdateMode.DISALLOW || shouldFail) {
			stpVer.verifyError();
		} else {
			stpVer.expectNext("empty", value, value, "empty").verifyComplete();
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsSet")
	public void testSetAndGetChanged(UpdateMode updateMode,
			String key,
			Object2ObjectSortedMap<String, String> value,
			boolean shouldFail) {
		var stpVer = StepVerifier
				.create(tempDb(getTempDbGenerator(), allocator, db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryDeepMap(dict, 5, 6))
						.flatMapMany(map -> Flux
								.concat(
										map.putValueAndGetChanged(key, new Object2ObjectLinkedOpenHashMap<>(Map.of("error?", "error."))).single(),
										map.putValueAndGetChanged(key, value).single(),
										map.putValueAndGetChanged(key, value).single(),
										map.remove(key),
										map.putValueAndGetChanged(key, new Object2ObjectLinkedOpenHashMap<>(Map.of("error?", "error."))).single()
								)
								.doFinally(s -> map.close())
						)
				));
		if (shouldFail) {
			this.checkLeaks = false;
			stpVer.verifyError();
		} else {
			stpVer.expectNext(true, true, false, true).verifyComplete();
		}
	}

	private static Stream<Arguments> provideArgumentsSetMulti() {
		var goodKeys = isCIMode() ? List.of(List.of("12345")) : List.of(List.of("12345", "67890"), List.<String>of());
		List<List<String>> badKeys;
		if (isTestBadKeysEnabled()) {
			badKeys = List.of(List.of("", "12345"), List.of("45678", "aaaa"), List.of("aaaaaa", "capra"));
		} else {
			badKeys = List.of();
		}
		List<Tuple2<List<String>, Boolean>> keys = Stream
				.concat(goodKeys.stream().map(s -> new Tuple2<>(s, false)), badKeys.stream().map(s -> new Tuple2<>(s, true)))
				.toList();
		var values = isCIMode() ? List.of(new Object2ObjectLinkedOpenHashMap<>(Map.of("123456", "val"))) : List.of(
				new Object2ObjectLinkedOpenHashMap<>(Map.of("123456", "a", "234567", "")),
				new Object2ObjectLinkedOpenHashMap<>(Map.of("123456", "\0", "234567", "\0\0", "345678", BIG_STRING))
		);

		return keys
				.stream()
				.map(keyTuple -> keyTuple.mapT1(ks -> Flux
						.zip(Flux.fromIterable(ks), Flux.fromIterable(values))
						.collectMap(Tuple2::getT1, Tuple2::getT2, Object2ObjectLinkedOpenHashMap::new)
						.block()
				))
				.flatMap(entryTuple -> Arrays.stream(UpdateMode.values()).map(updateMode -> new Tuple2<>(updateMode,
						entryTuple.getT1(),
						entryTuple.getT2()
				)))
				.map(fullTuple -> Arguments.of(fullTuple.getT1(), fullTuple.getT2(), fullTuple.getT3()));
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsSetMulti")
	public void testSetMultiGetMulti(UpdateMode updateMode,
			Map<String, Object2ObjectSortedMap<String, String>> entries,
			boolean shouldFail) {
		var flux = tempDb(getTempDbGenerator(), allocator, db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryDeepMap(dict, 5, 6))
						.flatMapMany(map -> {
							var entriesFlux = Flux.fromIterable(entries.entrySet());
							var keysFlux = entriesFlux.map(Entry::getKey);
							var resultsFlux = Flux
											.concat(
													map.putMulti(entriesFlux).then(Mono.empty()),
													map.getMulti(null, keysFlux)
											);
							return Flux.zip(keysFlux, resultsFlux, Map::entry).doFinally(s -> map.close());
						})
						.filter(k -> k.getValue().isPresent())
						.map(k -> Map.entry(k.getKey(), k.getValue().orElseThrow()))
				);
		if (shouldFail) {
			this.checkLeaks = false;
			StepVerifier.create(flux).verifyError();
		} else {
			var elements = flux.collect(Collectors.toList()).block();
			assertThat(elements).containsExactlyInAnyOrderElementsOf(entries.entrySet());
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsSetMulti")
	public void testSetAllValuesGetMulti(UpdateMode updateMode,
			Object2ObjectSortedMap<String, Object2ObjectSortedMap<String, String>> entries,
			boolean shouldFail) {
		var remainingEntries = new ConcurrentHashMap<Entry<String, Map<String, String>>, Boolean>().keySet(true);
		Step<Entry<String, Map<String, String>>> stpVer = StepVerifier
				.create(tempDb(getTempDbGenerator(), allocator, db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryDeepMap(dict, 5, 6))
						.flatMapMany(map -> {
							var entriesFlux = Flux.fromIterable(entries.entrySet());
							var keysFlux = entriesFlux.map(Entry::getKey);
							var resultsFlux = map
											.setAllValues(Flux.fromIterable(entries.entrySet()))
											.thenMany(map.getMulti(null, Flux.fromIterable(entries.keySet())));
							return Flux.zip(keysFlux, resultsFlux, Map::entry).doFinally(s -> map.close());
						})
						.filter(k -> k.getValue().isPresent())
						.map(k -> Map.entry(k.getKey(), k.getValue().orElseThrow()))
				));
		if (shouldFail) {
			this.checkLeaks = false;
			stpVer.verifyError();
		} else {
			entries.forEach((k, v) -> remainingEntries.add(Map.entry(k, v)));
			for (Entry<String, Map<String, String>> ignored : remainingEntries) {
				stpVer = stpVer.expectNextMatches(remainingEntries::remove);
			}
			stpVer.verifyComplete();
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsSetMulti")
	public void testSetAllValuesAndGetPrevious(UpdateMode updateMode, Object2ObjectSortedMap<String, Object2ObjectSortedMap<String, String>> entries,
			boolean shouldFail) {
		var remainingEntries = new ConcurrentHashMap<Entry<String, Object2ObjectSortedMap<String, String>>, Boolean>().keySet(true);
		Step<Entry<String, Object2ObjectSortedMap<String, String>>> stpVer = StepVerifier
				.create(tempDb(getTempDbGenerator(), allocator, db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryDeepMap(dict, 5, 6))
						.flatMapMany(map -> Flux
								.concat(
										map.setAllValuesAndGetPrevious(Flux.fromIterable(entries.entrySet())),
										map.setAllValuesAndGetPrevious(Flux.fromIterable(entries.entrySet()))
								)
								.doFinally(s -> map.close())
						)
				));
		if (shouldFail) {
			this.checkLeaks = false;
			stpVer.verifyError();
		} else {
			entries.forEach((k, v) -> remainingEntries.add(Map.entry(k, v)));
			for (Entry<String, Object2ObjectSortedMap<String, String>> ignored : remainingEntries) {
				stpVer = stpVer.expectNextMatches(remainingEntries::remove);
			}
			stpVer.verifyComplete();
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsSetMulti")
	public void testSetGetMulti(UpdateMode updateMode, Object2ObjectSortedMap<String, Object2ObjectSortedMap<String, String>> entries, boolean shouldFail) {
		var remainingEntries = new ConcurrentHashMap<Entry<String, Object2ObjectSortedMap<String, String>>, Boolean>().keySet(true);
		Step<Entry<String, Object2ObjectSortedMap<String, String>>> stpVer = StepVerifier
				.create(tempDb(getTempDbGenerator(), allocator, db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryDeepMap(dict, 5, 6))
						.flatMapMany(map -> {
							var entriesFlux = Flux.fromIterable(entries.entrySet());
							var keysFlux = entriesFlux.map(Entry::getKey);
							var resultsFlux = Flux
											.concat(
													map.set(entries).then(Mono.empty()),
													map.getMulti(null, Flux.fromIterable(entries.keySet()))
											);
							return Flux.zip(keysFlux, resultsFlux, Map::entry).doFinally(s -> map.close());
						})
						.filter(k -> k.getValue().isPresent())
						.map(k -> Map.entry(k.getKey(), k.getValue().orElseThrow()))
				));
		if (shouldFail) {
			this.checkLeaks = false;
			stpVer.verifyError();
		} else {
			entries.forEach((k, v) -> remainingEntries.add(Map.entry(k, v)));
			for (Entry<String, Object2ObjectSortedMap<String, String>> ignored : remainingEntries) {
				stpVer = stpVer.expectNextMatches(remainingEntries::remove);
			}
			stpVer.verifyComplete();
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsSetMulti")
	public void testSetAndGetStatus(UpdateMode updateMode, Object2ObjectSortedMap<String, Object2ObjectSortedMap<String, String>> entries,
			boolean shouldFail) {
		Step<Boolean> stpVer = StepVerifier
				.create(tempDb(getTempDbGenerator(), allocator, db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryDeepMap(dict, 5, 6))
						.flatMapMany(map -> {
							Mono<Void> removalMono;
							if (entries.isEmpty()) {
								removalMono = Mono.empty();
							} else {
								removalMono = map.remove(entries.keySet().stream().findAny().orElseThrow());
							}
							return Flux
									.concat(
											map.setAndGetChanged(entries).single(),
											map.setAndGetChanged(entries).single(),
											removalMono.then(Mono.empty()),
											map.setAndGetChanged(entries).single()
									)
									.doFinally(s -> map.close());
						})
				));
		if (shouldFail) {
			this.checkLeaks = false;
			stpVer.verifyError();
		} else {
			stpVer.expectNext(!entries.isEmpty(), false, !entries.isEmpty()).verifyComplete();
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsSetMulti")
	public void testSetAndGetPrevious(UpdateMode updateMode, Object2ObjectSortedMap<String, Object2ObjectSortedMap<String, String>> entries,
			boolean shouldFail) {
		var remainingEntries = new ConcurrentHashMap<Entry<String, Object2ObjectSortedMap<String, String>>, Boolean>().keySet(true);
		Step<Entry<String, Object2ObjectSortedMap<String, String>>> stpVer = StepVerifier
				.create(tempDb(getTempDbGenerator(), allocator, db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryDeepMap(dict, 5, 6))
						.flatMapMany(map -> Flux
								.concat(
										map.setAndGetPrevious(entries),
										map.setAndGetPrevious(entries)
								)
								.map(Map::entrySet)
								.concatMapIterable(list -> list)
								.doFinally(s -> map.close())
						)
				));
		if (shouldFail) {
			this.checkLeaks = false;
			stpVer.verifyError();
		} else {
			entries.forEach((k, v) -> remainingEntries.add(Map.entry(k, v)));
			for (Entry<String, Object2ObjectSortedMap<String, String>> ignored : remainingEntries) {
				stpVer = stpVer.expectNextMatches(remainingEntries::remove);
			}
			stpVer.verifyComplete();
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsSetMulti")
	public void testSetClearAndGetPreviousGet(UpdateMode updateMode, Object2ObjectSortedMap<String, Object2ObjectSortedMap<String, String>> entries,
			boolean shouldFail) {
		var remainingEntries = new ConcurrentHashMap<Entry<String, Object2ObjectSortedMap<String, String>>, Boolean>().keySet(true);
		Step<Entry<String, Object2ObjectSortedMap<String, String>>> stpVer = StepVerifier
				.create(tempDb(getTempDbGenerator(), allocator, db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryDeepMap(dict, 5, 6))
						.flatMapMany(map -> Flux
								.concat(map.set(entries).then(Mono.empty()), map.clearAndGetPrevious(), map.get(null))
								.map(Map::entrySet)
								.concatMapIterable(list -> list)
								.doFinally(s -> map.close())
						)
				));
		if (shouldFail) {
			this.checkLeaks = false;
			stpVer.verifyError();
		} else {
			entries.forEach((k, v) -> remainingEntries.add(Map.entry(k, v)));
			for (Entry<String, Object2ObjectSortedMap<String, String>> ignored : remainingEntries) {
				stpVer = stpVer.expectNextMatches(remainingEntries::remove);
			}
			stpVer.verifyComplete();
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsSetMulti")
	public void testSetMultiGetAllValues(UpdateMode updateMode, Object2ObjectSortedMap<String, Object2ObjectSortedMap<String, String>> entries,
			boolean shouldFail) {
		var remainingEntries = new ConcurrentHashMap<Entry<String, Object2ObjectSortedMap<String, String>>, Boolean>().keySet(true);
		Step<Entry<String, Object2ObjectSortedMap<String, String>>> stpVer = StepVerifier
				.create(tempDb(getTempDbGenerator(), allocator, db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryDeepMap(dict, 5, 6))
						.flatMapMany(map -> Flux
								.concat(
										map.putMulti(Flux.fromIterable(entries.entrySet())).then(Mono.empty()),
										map.getAllValues(null, false)
								)
								.doFinally(s -> map.close())
						)
				));
		if (shouldFail) {
			this.checkLeaks = false;
			stpVer.verifyError();
		} else {
			entries.forEach((k, v) -> remainingEntries.add(Map.entry(k, v)));
			for (Entry<String, Object2ObjectSortedMap<String, String>> ignored : remainingEntries) {
				stpVer = stpVer.expectNextMatches(remainingEntries::remove);
			}
			stpVer.verifyComplete();
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsSetMulti")
	public void testSetMultiGet(UpdateMode updateMode, Object2ObjectSortedMap<String, Object2ObjectSortedMap<String, String>> entries, boolean shouldFail) {
		var remainingEntries = new ConcurrentHashMap<Entry<String, Object2ObjectSortedMap<String, String>>, Boolean>().keySet(true);
		Step<Entry<String, Object2ObjectSortedMap<String, String>>> stpVer = StepVerifier
				.create(tempDb(getTempDbGenerator(), allocator, db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryDeepMap(dict, 5, 6))
						.flatMapMany(map -> Flux
								.concat(
										map.putMulti(Flux.fromIterable(entries.entrySet())).then(Mono.empty()),
										map.get(null)
										.map(Map::entrySet)
										.flatMapIterable(list -> list)
								)
								.doFinally(s -> map.close())
						)
				));
		if (shouldFail) {
			this.checkLeaks = false;
			stpVer.verifyError();
		} else {
			entries.forEach((k, v) -> remainingEntries.add(Map.entry(k, v)));
			for (Entry<String, Object2ObjectSortedMap<String, String>> ignored : remainingEntries) {
				stpVer = stpVer.expectNextMatches(remainingEntries::remove);
			}
			stpVer.verifyComplete();
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsSetMulti")
	public void testSetMultiGetAllStagesGet(UpdateMode updateMode, Object2ObjectSortedMap<String, Object2ObjectSortedMap<String, String>> entries,
			boolean shouldFail) {
		var remainingEntries = new ConcurrentHashMap<Entry<String, Object2ObjectSortedMap<String, String>>, Boolean>().keySet(true);
		Step<Entry<String, Object2ObjectSortedMap<String, String>>> stpVer = StepVerifier
				.create(tempDb(getTempDbGenerator(), allocator, db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryDeepMap(dict, 5, 6))
						.flatMapMany(map -> Flux
								.concat(
										map.putMulti(Flux.fromIterable(entries.entrySet())).then(Mono.empty()),
										map
												.getAllStages(null, false)
												.flatMap(stage -> stage
														.getValue()
														.get(null)
														.map(val -> Map.entry(stage.getKey(), val))
														.doFinally(s -> stage.getValue().close())
												)
								)
								.doFinally(s -> map.close())
						)
				));
		if (shouldFail) {
			this.checkLeaks = false;
			stpVer.verifyError();
		} else {
			entries.forEach((k, v) -> remainingEntries.add(Map.entry(k, v)));
			for (Entry<String, Object2ObjectSortedMap<String, String>> ignored : remainingEntries) {
				stpVer = stpVer.expectNextMatches(remainingEntries::remove);
			}
			stpVer.verifyComplete();
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsSetMulti")
	public void testSetMultiIsEmpty(UpdateMode updateMode, Object2ObjectSortedMap<String, Object2ObjectSortedMap<String, String>> entries,
			boolean shouldFail) {
		Step<Boolean> stpVer = StepVerifier
				.create(tempDb(getTempDbGenerator(), allocator, db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryDeepMap(dict, 5, 6))
						.flatMapMany(map -> Flux
								.concat(
										map.isEmpty(null),
										map.putMulti(Flux.fromIterable(entries.entrySet())).then(Mono.empty()),
										map.isEmpty(null)
								)
								.doFinally(s -> map.close())
						)
				));
		if (shouldFail) {
			this.checkLeaks = false;
			stpVer.expectNext(true).verifyError();
		} else {
			stpVer.expectNext(true, entries.isEmpty()).verifyComplete();
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsSetMulti")
	public void testSetMultiClear(UpdateMode updateMode, Object2ObjectSortedMap<String, Object2ObjectSortedMap<String, String>> entries, boolean shouldFail) {
		Step<Boolean> stpVer = StepVerifier
				.create(tempDb(getTempDbGenerator(), allocator, db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryDeepMap(dict, 5, 6))
						.flatMapMany(map -> Flux
								.concat(
										map.isEmpty(null),
										map.putMulti(Flux.fromIterable(entries.entrySet())).then(Mono.empty()),
										map.isEmpty(null),
										map.clear().then(Mono.empty()),
										map.isEmpty(null)
								)
								.doFinally(s -> map.close())
						)
				));
		if (shouldFail) {
			this.checkLeaks = false;
			stpVer.expectNext(true).verifyError();
		} else {
			stpVer.expectNext(true, entries.isEmpty(), true).verifyComplete();
		}
	}

 */
}
