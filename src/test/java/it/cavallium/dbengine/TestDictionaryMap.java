package it.cavallium.dbengine;

import static it.cavallium.dbengine.DbTestUtils.*;

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
import reactor.test.StepVerifier.Step;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

public class TestDictionaryMap {

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

	private static Stream<Arguments> provideArgumentsPut() {
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
		var values = Set.of("a", "", "\0", "\0\0", "z", "azzszgzczqz", BIG_STRING);

		return keys
				.stream()
				.flatMap(keyTuple -> {
					Stream<String> strm;
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

	@ParameterizedTest
	@MethodSource("provideArgumentsPut")
	public void testPut(UpdateMode updateMode, String key, String value, boolean shouldFail) {
		var stpVer = StepVerifier
				.create(tempDb(db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryMap(dict, 5))
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
	@MethodSource("provideArgumentsPut")
	public void testAtSetAtGet(UpdateMode updateMode, String key, String value, boolean shouldFail) {
		var stpVer = StepVerifier
				.create(tempDb(db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryMap(dict, 5))
						.flatMap(map -> map
								.at(null, key).flatMap(v -> v.set(value).doFinally(s -> v.release()))
								.then(map.at(null, key).flatMap(v -> v.get(null).doFinally(s -> v.release())))
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
	public void testPutAndGetPrevious(UpdateMode updateMode, String key, String value, boolean shouldFail) {
		var stpVer = StepVerifier
				.create(tempDb(db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryMap(dict, 5))
						.flatMapMany(map -> Flux
								.concat(
										map.putValueAndGetPrevious(key, "error?"),
										map.putValueAndGetPrevious(key, value),
										map.putValueAndGetPrevious(key, value)
								)
								.doFinally(s -> map.release())
						)
				));
		if (shouldFail) {
			stpVer.verifyError();
		} else {
			stpVer.expectNext("error?").expectNext(value).verifyComplete();
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsPut")
	public void testPutValueRemoveAndGetPrevious(UpdateMode updateMode, String key, String value, boolean shouldFail) {
		var stpVer = StepVerifier
				.create(tempDb(db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryMap(dict, 5))
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
	public void testPutValueRemoveAndGetStatus(UpdateMode updateMode, String key, String value, boolean shouldFail) {
		var stpVer = StepVerifier
				.create(tempDb(db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryMap(dict, 5))
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
	public void testUpdate(UpdateMode updateMode, String key, String value, boolean shouldFail) {
		if (updateMode == UpdateMode.DISALLOW && !isTestBadKeysEnabled()) {
			return;
		}
		var stpVer = StepVerifier
				.create(tempDb(db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryMap(dict, 5))
						.flatMapMany(map -> Flux
								.concat(
										map.updateValue(key, old -> {
											assert old == null;
											return "error?";
										}),
										map.updateValue(key, false, old -> {
											assert Objects.equals(old, "error?");
											return "error?";
										}),
										map.updateValue(key, true, old -> {
											assert Objects.equals(old, "error?");
											return "error?";
										}),
										map.updateValue(key, true, old -> {
											assert Objects.equals(old, "error?");
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
		if (updateMode == UpdateMode.DISALLOW || shouldFail) {
			stpVer.verifyError();
		} else {
			stpVer.expectNext(true, false, false, true, false).verifyComplete();
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsPut")
	public void testUpdateGet(UpdateMode updateMode, String key, String value, boolean shouldFail) {
		if (updateMode == UpdateMode.DISALLOW && !isTestBadKeysEnabled()) {
			return;
		}
		var stpVer = StepVerifier
				.create(tempDb(db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryMap(dict, 5))
						.flatMapMany(map -> Flux
								.concat(
										map.updateValue(key, old -> {
											assert old == null;
											return "error?";
										}).then(map.getValue(null, key)),
										map.updateValue(key, false, old -> {
											assert Objects.equals(old, "error?");
											return "error?";
										}).then(map.getValue(null, key)),
										map.updateValue(key, true, old -> {
											assert Objects.equals(old, "error?");
											return "error?";
										}).then(map.getValue(null, key)),
										map.updateValue(key, true, old -> {
											assert Objects.equals(old, "error?");
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
		if (updateMode == UpdateMode.DISALLOW || shouldFail) {
			stpVer.verifyError();
		} else {
			stpVer.expectNext("error?", "error?", "error?", value, value).verifyComplete();
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsPut")
	public void testPutAndGetChanged(UpdateMode updateMode, String key, String value, boolean shouldFail) {
		var stpVer = StepVerifier
				.create(tempDb(db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryMap(dict, 5))
						.flatMapMany(map -> Flux
								.concat(
										map.putValueAndGetChanged(key, "error?").single(),
										map.putValueAndGetChanged(key, value).single(),
										map.putValueAndGetChanged(key, value).single(),
										map.remove(key),
										map.putValueAndGetChanged(key, "error?").single()
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

	private static Stream<Arguments> provideArgumentsPutMulti() {
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
		var values = Set.of("a", "", "\0", "\0\0", "z", "azzszgzczqz", BIG_STRING);

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
	@MethodSource("provideArgumentsPutMulti")
	public void testPutMultiGetMulti(UpdateMode updateMode, Map<String, String> entries, boolean shouldFail) {
		var remainingEntries = new ConcurrentHashMap<Entry<String, String>, Boolean>().keySet(true);
		Step<Entry<String, String>> stpVer = StepVerifier
				.create(tempDb(db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryMap(dict, 5))
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
			for (Entry<String, String> ignored : remainingEntries) {
				stpVer = stpVer.expectNextMatches(remainingEntries::remove);
			}
			stpVer.verifyComplete();
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsPutMulti")
	public void testSetAllValuesGetMulti(UpdateMode updateMode, Map<String, String> entries, boolean shouldFail) {
		var remainingEntries = new ConcurrentHashMap<Entry<String, String>, Boolean>().keySet(true);
		Step<Entry<String, String>> stpVer = StepVerifier
				.create(tempDb(db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryMap(dict, 5))
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
			for (Entry<String, String> ignored : remainingEntries) {
				stpVer = stpVer.expectNextMatches(remainingEntries::remove);
			}
			stpVer.verifyComplete();
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsPutMulti")
	public void testSetAllValuesAndGetPrevious(UpdateMode updateMode, Map<String, String> entries, boolean shouldFail) {
		var remainingEntries = new ConcurrentHashMap<Entry<String, String>, Boolean>().keySet(true);
		Step<Entry<String, String>> stpVer = StepVerifier
				.create(tempDb(db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryMap(dict, 5))
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
			for (Entry<String, String> ignored : remainingEntries) {
				stpVer = stpVer.expectNextMatches(remainingEntries::remove);
			}
			stpVer.verifyComplete();
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsPutMulti")
	public void testSetGetMulti(UpdateMode updateMode, Map<String, String> entries, boolean shouldFail) {
		var remainingEntries = new ConcurrentHashMap<Entry<String, String>, Boolean>().keySet(true);
		Step<Entry<String, String>> stpVer = StepVerifier
				.create(tempDb(db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryMap(dict, 5))
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
			for (Entry<String, String> ignored : remainingEntries) {
				stpVer = stpVer.expectNextMatches(remainingEntries::remove);
			}
			stpVer.verifyComplete();
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsPutMulti")
	public void testSetAndGetChanged(UpdateMode updateMode, Map<String, String> entries, boolean shouldFail) {
		var remainingEntries = new ConcurrentHashMap<Entry<String, String>, Boolean>().keySet(true);
		Step<Boolean> stpVer = StepVerifier
				.create(tempDb(db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryMap(dict, 5))
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
	@MethodSource("provideArgumentsPutMulti")
	public void testSetAndGetPrevious(UpdateMode updateMode, Map<String, String> entries, boolean shouldFail) {
		var remainingEntries = new ConcurrentHashMap<Entry<String, String>, Boolean>().keySet(true);
		Step<Entry<String, String>> stpVer = StepVerifier
				.create(tempDb(db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryMap(dict, 5))
						.flatMapMany(map -> Flux
								.concat(map.setAndGetPrevious(entries), map.setAndGetPrevious(entries))
								.map(Map::entrySet)
								.flatMap(Flux::fromIterable)
								.doFinally(s -> map.release())
						)
				));
		if (shouldFail) {
			stpVer.verifyError();
		} else {
			entries.forEach((k, v) -> remainingEntries.add(Map.entry(k, v)));
			for (Entry<String, String> ignored : remainingEntries) {
				stpVer = stpVer.expectNextMatches(remainingEntries::remove);
			}
			stpVer.verifyComplete();
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsPutMulti")
	public void testSetClearAndGetPreviousGet(UpdateMode updateMode, Map<String, String> entries, boolean shouldFail) {
		var remainingEntries = new ConcurrentHashMap<Entry<String, String>, Boolean>().keySet(true);
		Step<Entry<String, String>> stpVer = StepVerifier
				.create(tempDb(db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryMap(dict, 5))
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
			for (Entry<String, String> ignored : remainingEntries) {
				stpVer = stpVer.expectNextMatches(remainingEntries::remove);
			}
			stpVer.verifyComplete();
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsPutMulti")
	public void testPutMultiGetAllValues(UpdateMode updateMode, Map<String, String> entries, boolean shouldFail) {
		var remainingEntries = new ConcurrentHashMap<Entry<String, String>, Boolean>().keySet(true);
		Step<Entry<String, String>> stpVer = StepVerifier
				.create(tempDb(db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryMap(dict, 5))
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
			for (Entry<String, String> ignored : remainingEntries) {
				stpVer = stpVer.expectNextMatches(remainingEntries::remove);
			}
			stpVer.verifyComplete();
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsPutMulti")
	public void testPutMultiGet(UpdateMode updateMode, Map<String, String> entries, boolean shouldFail) {
		var remainingEntries = new ConcurrentHashMap<Entry<String, String>, Boolean>().keySet(true);
		Step<Entry<String, String>> stpVer = StepVerifier
				.create(tempDb(db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryMap(dict, 5))
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
			for (Entry<String, String> ignored : remainingEntries) {
				stpVer = stpVer.expectNextMatches(remainingEntries::remove);
			}
			stpVer.verifyComplete();
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsPutMulti")
	public void testPutMultiGetAllStagesGet(UpdateMode updateMode, Map<String, String> entries, boolean shouldFail) {
		var remainingEntries = new ConcurrentHashMap<Entry<String, String>, Boolean>().keySet(true);
		Step<Entry<String, String>> stpVer = StepVerifier
				.create(tempDb(db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryMap(dict, 5))
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
			for (Entry<String, String> ignored : remainingEntries) {
				stpVer = stpVer.expectNextMatches(remainingEntries::remove);
			}
			stpVer.verifyComplete();
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsPutMulti")
	public void testPutMultiIsEmpty(UpdateMode updateMode, Map<String, String> entries, boolean shouldFail) {
		var remainingEntries = new ConcurrentHashMap<Entry<String, String>, Boolean>().keySet(true);
		Step<Boolean> stpVer = StepVerifier
				.create(tempDb(db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryMap(dict, 5))
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
	@MethodSource("provideArgumentsPutMulti")
	public void testPutMultiClear(UpdateMode updateMode, Map<String, String> entries, boolean shouldFail) {
		var remainingEntries = new ConcurrentHashMap<Entry<String, String>, Boolean>().keySet(true);
		Step<Boolean> stpVer = StepVerifier
				.create(tempDb(db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryMap(dict, 5))
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
