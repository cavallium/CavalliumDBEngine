package it.cavallium.dbengine;

import static it.cavallium.dbengine.DbTestUtils.tempDatabaseMapDictionaryDeepMap;
import static it.cavallium.dbengine.DbTestUtils.tempDb;
import static it.cavallium.dbengine.DbTestUtils.tempDictionary;

import it.cavallium.dbengine.database.UpdateMode;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.test.StepVerifier.FirstStep;
import reactor.test.StepVerifier.Step;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuple3;
import reactor.util.function.Tuple4;
import reactor.util.function.Tuples;

public class TestDictionaryMapDeep {

	private static boolean isTestBadKeysEnabled() {
		return System.getProperty("badkeys", "true").equalsIgnoreCase("true");
	}

	private static final String BIG_STRING
			= "01234567890123456789012345678901234567890123456789012345678901234567890123456789"
			+ "01234567890123456789012345678901234567890123456789012345678901234567890123456789"
			+ "01234567890123456789012345678901234567890123456789012345678901234567890123456789"
			+ "01234567890123456789012345678901234567890123456789012345678901234567890123456789"
			+ "01234567890123456789012345678901234567890123456789012345678901234567890123456789"
			+ "01234567890123456789012345678901234567890123456789012345678901234567890123456789";

	private static Stream<Arguments> provideArgumentsSet() {
		var goodKeys = Set.of("12345", "zebra");
		Set<String> badKeys;
		if (isTestBadKeysEnabled()) {
			badKeys = Set.of("", "a", "aaaa", "aaaaaa");
		} else {
			badKeys = Set.of();
		}
		Set<Tuple2<String, Boolean>> keys = Stream.concat(
				goodKeys.stream().map(s -> Tuples.of(s, false)),
				badKeys.stream().map(s -> Tuples.of(s, true))
		).collect(Collectors.toSet());
		var values = Set.of(
				Map.of("123456", "a", "234567", ""),
				Map.of("123456", "", "234567", "bb"),
				Map.of("123456", "\0", "234567", "\0\0", "345678", BIG_STRING)
		);

		return keys
				.stream()
				.flatMap(keyTuple -> {
					Stream<Map<String, String>> strm;
					if (keyTuple.getT2()) {
						strm = values.stream().limit(1);
					} else {
						strm = values.stream();
					}
					return strm.map(val -> Tuples.of(keyTuple.getT1(), val, keyTuple.getT2()));
				})
				.flatMap(entryTuple -> Arrays.stream(UpdateMode.values()).map(updateMode -> Tuples.of(updateMode,
						entryTuple.getT1(),
						entryTuple.getT2(),
						entryTuple.getT3()
				)))
				.map(fullTuple -> Arguments.of(fullTuple.getT1(), fullTuple.getT2(), fullTuple.getT3(), fullTuple.getT4()));
	}

	private static Stream<Arguments> provideArgumentsPut() {
		var goodKeys1 = Set.of("12345", "zebra");
		Set<String> badKeys1;
		if (isTestBadKeysEnabled()) {
			badKeys1 = Set.of("", "a", "aaaa", "aaaaaa");
		} else {
			badKeys1 = Set.of();
		}
		var goodKeys2 = Set.of("123456", "anatra");
		Set<String> badKeys2;
		if (isTestBadKeysEnabled()) {
			badKeys2 = Set.of("", "a", "aaaaa", "aaaaaaa");
		} else {
			badKeys2 = Set.of();
		}

		var values = Set.of("a", "", "\0", "\0\0", "z", "azzszgzczqz", BIG_STRING);

		Flux<Tuple4<String, String, String, Boolean>> failOnKeys1 = Flux
				.fromIterable(badKeys1)
				.map(badKey1 -> Tuples.of(
						badKey1,
						goodKeys2.stream().findAny().orElseThrow(),
						values.stream().findAny().orElseThrow(),
						true
				));
		Flux<Tuple4<String, String, String, Boolean>> failOnKeys2 = Flux
				.fromIterable(badKeys2)
				.map(badKey2 -> Tuples.of(
						goodKeys1.stream().findAny().orElseThrow(),
						badKey2,
						values.stream().findAny().orElseThrow(),
						true
				));

		Flux<Tuple4<String, String, String, Boolean>> goodKeys1And2 = Flux
				.fromIterable(values)
				.map(value -> Tuples.of(
						goodKeys1.stream().findAny().orElseThrow(),
						goodKeys2.stream().findAny().orElseThrow(),
						value,
						false
				));

		Flux<Tuple4<String, String, String, Boolean>> keys1And2 = Flux
				.concat(
						goodKeys1And2,
						failOnKeys1,
						failOnKeys2
				);

		return keys1And2
				.flatMap(entryTuple -> Flux
						.fromArray(UpdateMode.values())
						.map(updateMode -> Tuples.of(updateMode,
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
				.toStream();
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsSet")
	public void testSetValueGetValue(UpdateMode updateMode, String key, Map<String, String> value, boolean shouldFail) {
		var stpVer = StepVerifier
				.create(tempDb(db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryDeepMap(dict, 5, 6))
						.flatMap(map -> map
								.putValue(key, value)
								.then(map.getValue(null, key))
								.doFinally(s -> map.release())
						)
				));
		if (shouldFail) {
			stpVer.verifyError();
		} else {
			stpVer.expectNext(value).verifyComplete();
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsSet")
	public void testSetValueGetAllValues(UpdateMode updateMode,
			String key,
			Map<String, String> value,
			boolean shouldFail) {
		var stpVer = StepVerifier
				.create(tempDb(db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryDeepMap(dict, 5, 6))
						.flatMapMany(map -> map
								.putValue(key, value)
								.thenMany(map.getAllValues(null))
								.doFinally(s -> map.release())
						)
				));
		if (shouldFail) {
			stpVer.verifyError();
		} else {
			stpVer.expectNext(Map.entry(key, value)).verifyComplete();
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsSet")
	public void testAtSetGetAllStagesGetAllValues(UpdateMode updateMode, String key, Map<String, String> value, boolean shouldFail) {
		var remainingEntries = new ConcurrentHashMap<Tuple3<String, String, String>, Boolean>().keySet(true);
		Step<Tuple3<String, String, String>> stpVer = StepVerifier
				.create(tempDb(db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryDeepMap(dict, 5, 6))
						.flatMapMany(map -> map
								.at(null, key)
								.flatMap(v -> v
										.set(value)
										.doFinally(s -> v.release())
								)
								.then(map
										.at(null, "capra")
										.flatMap(v -> v
												.set(Map.of("normal", "123", "ormaln", "456"))
												.doFinally(s -> v.release())
										)
								)
								.thenMany(map
										.getAllStages(null)
										.flatMap(v -> v.getValue()
												.getAllValues(null)
												.map(result -> Tuples.of(v.getKey(), result.getKey(), result.getValue()))
												.doFinally(s -> v.getValue().release())
										)
								)
								.doFinally(s -> map.release())
						)
				));
		if (shouldFail) {
			stpVer.verifyError();
		} else {
			value.forEach((k, v) -> remainingEntries.add(Tuples.of(key, k, v)));
			remainingEntries.add(Tuples.of("capra", "normal", "123"));
			remainingEntries.add(Tuples.of("capra", "ormaln", "456"));
			for (Tuple3<String, String, String> ignored : remainingEntries) {
				stpVer = stpVer.expectNextMatches(remainingEntries::remove);
			}
			stpVer.verifyComplete();
			assert remainingEntries.isEmpty();
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsPut")
	public void testAtPutValueAtGetValue(UpdateMode updateMode, String key1, String key2, String value, boolean shouldFail) {
		var stpVer = StepVerifier
				.create(tempDb(db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryDeepMap(dict, 5, 6))
						.flatMap(map -> map
								.at(null, key1).flatMap(v -> v.putValue(key2, value).doFinally(s -> v.release()))
								.then(map.at(null, key1).flatMap(v -> v.getValue(null, key2).doFinally(s -> v.release())))
								.doFinally(s -> map.release())
						)
				));
		if (shouldFail) {
			stpVer.verifyError();
		} else {
			stpVer.expectNext(value).verifyComplete();
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsSet")
	public void testSetAndGetPrevious(UpdateMode updateMode, String key, Map<String, String> value, boolean shouldFail) {
		var stpVer = StepVerifier
				.create(tempDb(db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryDeepMap(dict, 5, 6))
						.flatMapMany(map -> Flux
								.concat(
										map
												.putValueAndGetPrevious(key, Map.of("error?", "error."))
												.defaultIfEmpty(Map.of("nothing", "nothing")),
										map.putValueAndGetPrevious(key, value),
										map.putValueAndGetPrevious(key, value)
								)
								.doFinally(s -> map.release())
						)
				));
		if (shouldFail) {
			stpVer.verifyError();
		} else {
			stpVer.expectNext(Map.of("nothing", "nothing"), Map.of("error?", "error.")).expectNext(value).verifyComplete();
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsPut")
	public void testAtPutValueAndGetPrevious(UpdateMode updateMode, String key1, String key2, String value, boolean shouldFail) {
		var stpVer = StepVerifier
				.create(tempDb(db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryDeepMap(dict, 5, 6))
						.flatMapMany(map -> Flux
								.concat(
										map
												.at(null, key1)
												.flatMap(v -> v
														.putValueAndGetPrevious(key2, "error?")
														.doFinally(s -> v.release())
												),
										map
												.at(null, key1)
												.flatMap(v -> v
														.putValueAndGetPrevious(key2, value)
														.doFinally(s -> v.release())
												),
										map
												.at(null, key1)
												.flatMap(v -> v
														.putValueAndGetPrevious(key2, value)
														.doFinally(s -> v.release())
												)
								)
								.doFinally(s -> map.release())
						)
				));
		if (shouldFail) {
			stpVer.verifyError();
		} else {
			stpVer.expectNext("error?", value).verifyComplete();
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsSet")
	public void testSetValueRemoveAndGetPrevious(UpdateMode updateMode, String key, Map<String, String> value, boolean shouldFail) {
		var stpVer = StepVerifier
				.create(tempDb(db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryDeepMap(dict, 5, 6))
						.flatMapMany(map -> Flux
								.concat(
										map.removeAndGetPrevious(key),
										map.putValue(key, value).then(map.removeAndGetPrevious(key)),
										map.removeAndGetPrevious(key)
								)
								.doFinally(s -> map.release())
						)
				));
		if (shouldFail) {
			stpVer.verifyError();
		} else {
			stpVer.expectNext(value).verifyComplete();
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsPut")
	public void testAtPutValueRemoveAndGetPrevious(UpdateMode updateMode, String key1, String key2, String value, boolean shouldFail) {
		var stpVer = StepVerifier
				.create(tempDb(db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryDeepMap(dict, 5, 6))
						.flatMapMany(map -> Flux
								.concat(
										map
												.at(null, key1)
												.flatMap(v -> v
														.putValue(key2, "error?")
														.then(v.removeAndGetPrevious(key2))
														.doFinally(s -> v.release())
												),
										map
												.at(null, key1)
												.flatMap(v -> v
														.putValue(key2, value)
														.then(v.removeAndGetPrevious(key2))
														.doFinally(s -> v.release())
												),
										map
												.at(null, key1)
												.flatMap(v -> v.removeAndGetPrevious(key2)
														.doFinally(s -> v.release())
												)
								)
								.doFinally(s -> map.release())
						)
				));
		if (shouldFail) {
			stpVer.verifyError();
		} else {
			stpVer.expectNext("error?", value).verifyComplete();
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsSet")
	public void testSetValueRemoveAndGetStatus(UpdateMode updateMode, String key, Map<String, String> value, boolean shouldFail) {
		var stpVer = StepVerifier
				.create(tempDb(db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryDeepMap(dict, 5, 6))
						.flatMapMany(map -> Flux
								.concat(
										map.removeAndGetStatus(key),
										map.putValue(key, value).then(map.removeAndGetStatus(key)),
										map.removeAndGetStatus(key)
								)
								.doFinally(s -> map.release())
						)
				));
		if (shouldFail) {
			stpVer.verifyError();
		} else {
			stpVer.expectNext(false, true, false).verifyComplete();
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsPut")
	public void testAtPutValueRemoveAndGetStatus(UpdateMode updateMode, String key1, String key2, String value, boolean shouldFail) {
		var stpVer = StepVerifier
				.create(tempDb(db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryDeepMap(dict, 5, 6))
						.flatMapMany(map -> Flux
								.concat(
										map
												.at(null, key1)
												.flatMap(v -> v
														.putValue(key2, "error?")
														.then(v.removeAndGetStatus(key2))
														.doFinally(s -> v.release())
												),
										map
												.at(null, key1)
												.flatMap(v -> v
														.putValue(key2, value)
														.then(v.removeAndGetStatus(key2))
														.doFinally(s -> v.release())
												),
										map
												.at(null, key1)
												.flatMap(v -> v.removeAndGetStatus(key2)
														.doFinally(s -> v.release())
												)
								)
								.doFinally(s -> map.release())
						)
				));
		if (shouldFail) {
			stpVer.verifyError();
		} else {
			stpVer.expectNext(true, true, false).verifyComplete();
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsSet")
	public void testUpdate(UpdateMode updateMode, String key, Map<String, String> value, boolean shouldFail) {
		if (updateMode != UpdateMode.ALLOW_UNSAFE && !isTestBadKeysEnabled()) {
			return;
		}
		var stpVer = StepVerifier
				.create(tempDb(db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryDeepMap(dict, 5, 6))
						.flatMapMany(map -> Flux
								.concat(
										map.updateValue(key, old -> {
											assert old == null;
											return Map.of("error?", "error.");
										}),
										map.updateValue(key, false, old -> {
											assert Objects.equals(old, Map.of("error?", "error."));
											return Map.of("error?", "error.");
										}),
										map.updateValue(key, true, old -> {
											assert Objects.equals(old, Map.of("error?", "error."));
											return Map.of("error?", "error.");
										}),
										map.updateValue(key, true, old -> {
											assert Objects.equals(old, Map.of("error?", "error."));
											return value;
										}),
										map.updateValue(key, true, old -> {
											assert Objects.equals(old, value);
											return value;
										})
								)
								.doFinally(s -> map.release())
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
				.create(tempDb(db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryDeepMap(dict, 5, 6))
						.flatMapMany(map -> Flux
								.concat(
										map
												.at(null, key1)
												.flatMap(v -> v
														.updateValue(key2, prev -> prev)
														.doFinally(s -> v.release())
												),
										map
												.at(null, key1)
												.flatMap(v -> v
														.updateValue(key2, prev -> value)
														.doFinally(s -> v.release())
												),
										map
												.at(null, key1)
												.flatMap(v -> v
														.updateValue(key2, prev -> value)
														.doFinally(s -> v.release())
												),
										map
												.at(null, key1)
												.flatMap(v -> v
														.updateValue(key2, prev -> null)
														.doFinally(s -> v.release())
												)
								)
								.doFinally(s -> map.release())
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
	public void testUpdateGet(UpdateMode updateMode, String key, Map<String, String> value, boolean shouldFail) {
		if (updateMode != UpdateMode.ALLOW_UNSAFE && !isTestBadKeysEnabled()) {
			return;
		}
		var stpVer = StepVerifier
				.create(tempDb(db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryDeepMap(dict, 5, 6))
						.flatMapMany(map -> Flux
								.concat(
										map.updateValue(key, old -> {
											assert old == null;
											return Map.of("error?", "error.");
										}).then(map.getValue(null, key)),
										map.updateValue(key, false, old -> {
											assert Objects.equals(old, Map.of("error?", "error."));
											return Map.of("error?", "error.");
										}).then(map.getValue(null, key)),
										map.updateValue(key, true, old -> {
											assert Objects.equals(old, Map.of("error?", "error."));
											return Map.of("error?", "error.");
										}).then(map.getValue(null, key)),
										map.updateValue(key, true, old -> {
											assert Objects.equals(old, Map.of("error?", "error."));
											return value;
										}).then(map.getValue(null, key)),
										map.updateValue(key, true, old -> {
											assert Objects.equals(old, value);
											return value;
										}).then(map.getValue(null, key))
								)
								.doFinally(s -> map.release())
						)
				));
		if (updateMode != UpdateMode.ALLOW_UNSAFE || shouldFail) {
			stpVer.verifyError();
		} else {
			stpVer.expectNext(Map.of("error?", "error."), Map.of("error?", "error."), Map.of("error?", "error."), value, value).verifyComplete();
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsPut")
	public void testAtUpdateGetValue(UpdateMode updateMode, String key1, String key2, String value, boolean shouldFail) {
		if (updateMode == UpdateMode.DISALLOW && !isTestBadKeysEnabled()) {
			return;
		}
		var stpVer = StepVerifier
				.create(tempDb(db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryDeepMap(dict, 5, 6))
						.flatMapMany(map -> Flux
								.concat(
										map
												.at(null, key1)
												.flatMap(v -> v
														.updateValue(key2, prev -> prev)
														.then(v.getValue(null, key2))
														.defaultIfEmpty("empty")
														.doFinally(s -> v.release())
												),
										map
												.at(null, key1)
												.flatMap(v -> v
														.updateValue(key2, prev -> value)
														.then(v.getValue(null, key2))
														.defaultIfEmpty("empty")
														.doFinally(s -> v.release())
												),
										map
												.at(null, key1)
												.flatMap(v -> v
														.updateValue(key2, prev -> value)
														.then(v.getValue(null, key2))
														.defaultIfEmpty("empty")
														.doFinally(s -> v.release())
												),
										map
												.at(null, key1)
												.flatMap(v -> v
														.updateValue(key2, prev -> null)
														.then(v.getValue(null, key2))
														.defaultIfEmpty("empty")
														.doFinally(s -> v.release())
												)
								)
								.doFinally(s -> map.release())
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
	public void testSetAndGetChanged(UpdateMode updateMode, String key, Map<String, String> value, boolean shouldFail) {
		var stpVer = StepVerifier
				.create(tempDb(db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryDeepMap(dict, 5, 6))
						.flatMapMany(map -> Flux
								.concat(
										map.putValueAndGetChanged(key, Map.of("error?", "error.")).single(),
										map.putValueAndGetChanged(key, value).single(),
										map.putValueAndGetChanged(key, value).single(),
										map.remove(key),
										map.putValueAndGetChanged(key, Map.of("error?", "error.")).single()
								)
								.doFinally(s -> map.release())
						)
				));
		if (shouldFail) {
			stpVer.verifyError();
		} else {
			stpVer.expectNext(true, true, false, true).verifyComplete();
		}
	}

	private static Stream<Arguments> provideArgumentsSetMulti() {
		var goodKeys = Set.of(Set.of("12345", "67890"), Set.of("zebra"), Set.<String>of());
		Set<Set<String>> badKeys;
		if (isTestBadKeysEnabled()) {
			badKeys = Set.of(Set.of("", "12345"), Set.of("12345", "a"), Set.of("45678", "aaaa"), Set.of("aaaaaa", "capra"));
		} else {
			badKeys = Set.of();
		}
		Set<Tuple2<Set<String>, Boolean>> keys = Stream.concat(
				goodKeys.stream().map(s -> Tuples.of(s, false)),
				badKeys.stream().map(s -> Tuples.of(s, true))
		).collect(Collectors.toSet());
		var values = Set.of(
				Map.of("123456", "a", "234567", ""),
				Map.of("123456", "", "234567", "bb"),
				Map.of("123456", "\0", "234567", "\0\0", "345678", BIG_STRING)
		);

		return keys
				.stream()
				.map(keyTuple -> keyTuple.mapT1(ks -> Flux
						.zip(Flux.fromIterable(ks), Flux.fromIterable(values))
						.collectMap(Tuple2::getT1, Tuple2::getT2)
						.block()
				))
				.flatMap(entryTuple -> Arrays.stream(UpdateMode.values()).map(updateMode -> Tuples.of(updateMode,
						entryTuple.getT1(),
						entryTuple.getT2()
				)))
				.map(fullTuple -> Arguments.of(fullTuple.getT1(), fullTuple.getT2(), fullTuple.getT3()));
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsSetMulti")
	public void testSetMultiGetMulti(UpdateMode updateMode, Map<String, Map<String, String>> entries, boolean shouldFail) {
		var remainingEntries = new ConcurrentHashMap<Entry<String, Map<String, String>>, Boolean>().keySet(true);
		Step<Entry<String, Map<String, String>>> stpVer = StepVerifier
				.create(tempDb(db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryDeepMap(dict, 5, 6))
						.flatMapMany(map -> Flux
								.concat(
										map.putMulti(Flux.fromIterable(entries.entrySet())).then(Mono.empty()),
										map.getMulti(null, Flux.fromIterable(entries.keySet()))
								)
								.doFinally(s -> map.release())
						)
				));
		if (shouldFail) {
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
	public void testSetAllValuesGetMulti(UpdateMode updateMode, Map<String, Map<String, String>> entries, boolean shouldFail) {
		var remainingEntries = new ConcurrentHashMap<Entry<String, Map<String, String>>, Boolean>().keySet(true);
		Step<Entry<String, Map<String, String>>> stpVer = StepVerifier
				.create(tempDb(db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryDeepMap(dict, 5, 6))
						.flatMapMany(map -> map
								.setAllValues(Flux.fromIterable(entries.entrySet()))
								.thenMany(map.getMulti(null, Flux.fromIterable(entries.keySet())))
								.doFinally(s -> map.release())
						)
				));
		if (shouldFail) {
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
	public void testSetAllValuesAndGetPrevious(UpdateMode updateMode, Map<String, Map<String, String>> entries, boolean shouldFail) {
		var remainingEntries = new ConcurrentHashMap<Entry<String, Map<String, String>>, Boolean>().keySet(true);
		Step<Entry<String, Map<String, String>>> stpVer = StepVerifier
				.create(tempDb(db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryDeepMap(dict, 5, 6))
						.flatMapMany(map -> Flux
								.concat(
										map.setAllValuesAndGetPrevious(Flux.fromIterable(entries.entrySet())),
										map.setAllValuesAndGetPrevious(Flux.fromIterable(entries.entrySet()))
								)
								.doFinally(s -> map.release())
						)
				));
		if (shouldFail) {
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
	public void testSetGetMulti(UpdateMode updateMode, Map<String, Map<String, String>> entries, boolean shouldFail) {
		var remainingEntries = new ConcurrentHashMap<Entry<String, Map<String, String>>, Boolean>().keySet(true);
		Step<Entry<String, Map<String, String>>> stpVer = StepVerifier
				.create(tempDb(db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryDeepMap(dict, 5, 6))
						.flatMapMany(map -> Flux
								.concat(
										map.set(entries).then(Mono.empty()),
										map.getMulti(null, Flux.fromIterable(entries.keySet()))
								)
								.doFinally(s -> map.release())
						)
				));
		if (shouldFail) {
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
	public void testSetAndGetStatus(UpdateMode updateMode, Map<String, Map<String, String>> entries, boolean shouldFail) {
		Step<Boolean> stpVer = StepVerifier
				.create(tempDb(db -> tempDictionary(db, updateMode)
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
									.doFinally(s -> map.release());
						})
				));
		if (shouldFail) {
			stpVer.verifyError();
		} else {
			stpVer.expectNext(!entries.isEmpty(), false, !entries.isEmpty()).verifyComplete();
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsSetMulti")
	public void testSetAndGetPrevious(UpdateMode updateMode, Map<String, Map<String, String>> entries, boolean shouldFail) {
		var remainingEntries = new ConcurrentHashMap<Entry<String, Map<String, String>>, Boolean>().keySet(true);
		Step<Entry<String, Map<String, String>>> stpVer = StepVerifier
				.create(tempDb(db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryDeepMap(dict, 5, 6))
						.flatMapMany(map -> Flux
								.concat(
										map.setAndGetPrevious(entries),
										map.setAndGetPrevious(entries)
								)
								.map(Map::entrySet)
								.flatMap(Flux::fromIterable)
								.doFinally(s -> map.release())
						)
				));
		if (shouldFail) {
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
	public void testSetClearAndGetPreviousGet(UpdateMode updateMode, Map<String, Map<String, String>> entries, boolean shouldFail) {
		var remainingEntries = new ConcurrentHashMap<Entry<String, Map<String, String>>, Boolean>().keySet(true);
		Step<Entry<String, Map<String, String>>> stpVer = StepVerifier
				.create(tempDb(db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryDeepMap(dict, 5, 6))
						.flatMapMany(map -> Flux
								.concat(map.set(entries).then(Mono.empty()), map.clearAndGetPrevious(), map.get(null))
								.map(Map::entrySet)
								.flatMap(Flux::fromIterable)
								.doFinally(s -> map.release())
						)
				));
		if (shouldFail) {
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
	public void testSetMultiGetAllValues(UpdateMode updateMode, Map<String, Map<String, String>> entries, boolean shouldFail) {
		var remainingEntries = new ConcurrentHashMap<Entry<String, Map<String, String>>, Boolean>().keySet(true);
		Step<Entry<String, Map<String, String>>> stpVer = StepVerifier
				.create(tempDb(db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryDeepMap(dict, 5, 6))
						.flatMapMany(map -> Flux
								.concat(
										map.putMulti(Flux.fromIterable(entries.entrySet())).then(Mono.empty()),
										map.getAllValues(null)
								)
								.doFinally(s -> map.release())
						)
				));
		if (shouldFail) {
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
	public void testSetMultiGet(UpdateMode updateMode, Map<String, Map<String, String>> entries, boolean shouldFail) {
		var remainingEntries = new ConcurrentHashMap<Entry<String, Map<String, String>>, Boolean>().keySet(true);
		Step<Entry<String, Map<String, String>>> stpVer = StepVerifier
				.create(tempDb(db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryDeepMap(dict, 5, 6))
						.flatMapMany(map -> Flux
								.concat(
										map.putMulti(Flux.fromIterable(entries.entrySet())).then(Mono.empty()),
										map.get(null)
										.map(Map::entrySet)
										.flatMapMany(Flux::fromIterable)
								)
								.doFinally(s -> map.release())
						)
				));
		if (shouldFail) {
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
	public void testSetMultiGetAllStagesGet(UpdateMode updateMode, Map<String, Map<String, String>> entries, boolean shouldFail) {
		var remainingEntries = new ConcurrentHashMap<Entry<String, Map<String, String>>, Boolean>().keySet(true);
		Step<Entry<String, Map<String, String>>> stpVer = StepVerifier
				.create(tempDb(db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryDeepMap(dict, 5, 6))
						.flatMapMany(map -> Flux
								.concat(
										map.putMulti(Flux.fromIterable(entries.entrySet())).then(Mono.empty()),
										map
												.getAllStages(null)
												.flatMap(stage -> stage
														.getValue()
														.get(null)
														.map(val -> Map.entry(stage.getKey(), val))
														.doFinally(s -> stage.getValue().release())
												)
								)
								.doFinally(s -> map.release())
						)
				));
		if (shouldFail) {
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
	public void testSetMultiIsEmpty(UpdateMode updateMode, Map<String, Map<String, String>> entries, boolean shouldFail) {
		Step<Boolean> stpVer = StepVerifier
				.create(tempDb(db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryDeepMap(dict, 5, 6))
						.flatMapMany(map -> Flux
								.concat(
										map.isEmpty(null),
										map.putMulti(Flux.fromIterable(entries.entrySet())).then(Mono.empty()),
										map.isEmpty(null)
								)
								.doFinally(s -> map.release())
						)
				));
		if (shouldFail) {
			stpVer.expectNext(true).verifyError();
		} else {
			stpVer.expectNext(true, entries.isEmpty()).verifyComplete();
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsSetMulti")
	public void testSetMultiClear(UpdateMode updateMode, Map<String, Map<String, String>> entries, boolean shouldFail) {
		Step<Boolean> stpVer = StepVerifier
				.create(tempDb(db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryDeepMap(dict, 5, 6))
						.flatMapMany(map -> Flux
								.concat(
										map.isEmpty(null),
										map.putMulti(Flux.fromIterable(entries.entrySet())).then(Mono.empty()),
										map.isEmpty(null),
										map.clear().then(Mono.empty()),
										map.isEmpty(null)
								)
								.doFinally(s -> map.release())
						)
				));
		if (shouldFail) {
			stpVer.expectNext(true).verifyError();
		} else {
			stpVer.expectNext(true, entries.isEmpty(), true).verifyComplete();
		}
	}
}
