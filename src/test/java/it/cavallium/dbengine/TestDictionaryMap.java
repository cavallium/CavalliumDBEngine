package it.cavallium.dbengine;

import static it.cavallium.dbengine.DbTestUtils.*;

import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.UpdateMode;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
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

	private TestAllocator allocator;

	private static boolean isTestBadKeysEnabled() {
		return System.getProperty("badkeys", "true").equalsIgnoreCase("true");
	}

	private static final String BIG_STRING = "012345678901234567890123456789";

	private static Stream<Arguments> provideArgumentsPut() {
		var goodKeys = List.of("12345");
		List<String> badKeys;
		if (isTestBadKeysEnabled()) {
			badKeys = List.of("", "aaaa", "aaaaaa");
		} else {
			badKeys = List.of();
		}
		List<Tuple2<String, Boolean>> keys = Stream.concat(
				goodKeys.stream().map(s -> Tuples.of(s, false)),
				badKeys.stream().map(s -> Tuples.of(s, true))
		).collect(Collectors.toList());
		var values = List.of("", "\0", BIG_STRING);

		return keys
				.stream()
				.flatMap(keyTuple -> {
					Stream<String> strm;
					if (keyTuple.getT2()) {
						strm = values.stream().findFirst().stream();
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
				.flatMap(entryTuple -> Stream.of(Tuples.of(DbType.MAP, entryTuple.getT1(),
						entryTuple.getT2(),
						entryTuple.getT3(),
						entryTuple.getT4()
				), Tuples.of(DbType.HASH_MAP, entryTuple.getT1(),
						entryTuple.getT2(),
						entryTuple.getT3(),
						false
				)))
				.filter(tuple -> !(tuple.getT1() == DbType.HASH_MAP && tuple.getT2() != UpdateMode.ALLOW))
				.map(fullTuple -> Arguments.of(fullTuple.getT1(), fullTuple.getT2(), fullTuple.getT3(), fullTuple.getT4(), fullTuple.getT5()));
	}

	@BeforeEach
	public void beforeEach() {
		this.allocator = newAllocator();
		ensureNoLeaks(allocator.allocator(), false);
	}

	@AfterEach
	public void afterEach() {
		ensureNoLeaks(allocator.allocator(), true);
		destroyAllocator(allocator);
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsPut")
	public void testPut(DbType dbType, UpdateMode updateMode, String key, String value, boolean shouldFail) {
		var stpVer = StepVerifier
				.create(tempDb(allocator, db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryMap(dict, dbType, 5))
						.flatMap(map -> map
								.putValue(key, value)
								.then(map.getValue(null, key))
								.doAfterTerminate(map::release)
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
	public void testAtSetAtGet(DbType dbType, UpdateMode updateMode, String key, String value, boolean shouldFail) {
		var stpVer = StepVerifier
				.create(tempDb(allocator, db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryMap(dict, dbType, 5))
						.flatMap(map -> map
								.at(null, key).flatMap(v -> v.set(value).doAfterTerminate(v::release))
								.then(map.at(null, key).flatMap(v -> v.get(null).doAfterTerminate(v::release)))
								.doAfterTerminate(map::release)
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
	public void testPutAndGetPrevious(DbType dbType, UpdateMode updateMode, String key, String value, boolean shouldFail) {
		var stpVer = StepVerifier
				.create(tempDb(allocator, db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryMap(dict, dbType, 5))
						.flatMapMany(map -> Flux
								.concat(
										map.putValueAndGetPrevious(key, "error?"),
										map.putValueAndGetPrevious(key, value),
										map.putValueAndGetPrevious(key, value)
								)
								.doAfterTerminate(map::release)
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
	public void testPutValueRemoveAndGetPrevious(DbType dbType, UpdateMode updateMode, String key, String value, boolean shouldFail) {
		var stpVer = StepVerifier
				.create(tempDb(allocator, db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryMap(dict, dbType, 5))
						.flatMapMany(map -> Flux
								.concat(
										map.removeAndGetPrevious(key),
										map.putValue(key, value).then(map.removeAndGetPrevious(key)),
										map.removeAndGetPrevious(key)
								)
								.doAfterTerminate(map::release)
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
	public void testPutValueRemoveAndGetStatus(DbType dbType, UpdateMode updateMode, String key, String value, boolean shouldFail) {
		var stpVer = StepVerifier
				.create(tempDb(allocator, db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryMap(dict, dbType, 5))
						.flatMapMany(map -> Flux
								.concat(
										map.removeAndGetStatus(key),
										map.putValue(key, value).then(map.removeAndGetStatus(key)),
										map.removeAndGetStatus(key)
								)
								.doAfterTerminate(map::release)
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
	public void testUpdate(DbType dbType, UpdateMode updateMode, String key, String value, boolean shouldFail) {
		if (updateMode == UpdateMode.DISALLOW && !isTestBadKeysEnabled()) {
			return;
		}
		var stpVer = StepVerifier
				.create(tempDb(allocator, db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryMap(dict, dbType, 5))
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
								.doAfterTerminate(map::release)
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
	public void testUpdateGet(DbType dbType, UpdateMode updateMode, String key, String value, boolean shouldFail) {
		if (updateMode == UpdateMode.DISALLOW && !isTestBadKeysEnabled()) {
			return;
		}
		var stpVer = StepVerifier
				.create(tempDb(allocator, db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryMap(dict, dbType, 5))
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
								.doAfterTerminate(map::release)
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
	public void testPutAndGetChanged(DbType dbType, UpdateMode updateMode, String key, String value, boolean shouldFail) {
		var stpVer = StepVerifier
				.create(tempDb(allocator, db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryMap(dict, dbType, 5))
						.flatMapMany(map -> Flux
								.concat(
										map.putValueAndGetChanged(key, "error?").single(),
										map.putValueAndGetChanged(key, value).single(),
										map.putValueAndGetChanged(key, value).single(),
										map.remove(key),
										map.putValueAndGetChanged(key, "error?").single()
								)
								.doAfterTerminate(map::release)
						)
				));
		if (shouldFail) {
			stpVer.verifyError();
		} else {
			stpVer.expectNext(true, true, false, true).verifyComplete();
		}
	}

	private static Stream<Arguments> provideArgumentsPutMulti() {
		var goodKeys = List.of(List.of("12345", "67890"), List.<String>of());
		List<List<String>> badKeys;
		if (isTestBadKeysEnabled()) {
			badKeys = List.of(List.of("", "12345"), List.of("45678", "aaaa"), List.of("aaaaaa", "capra"));
		} else {
			badKeys = List.of();
		}
		List<Tuple2<List<String>, Boolean>> keys = Stream.concat(
				goodKeys.stream().map(s -> Tuples.of(s, false)),
				badKeys.stream().map(s -> Tuples.of(s, true))
		).collect(Collectors.toList());
		var values = List.of("", "\0", BIG_STRING);

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
				.flatMap(entryTuple -> Stream.of(Tuples.of(DbType.MAP, entryTuple.getT1(),
						entryTuple.getT2(),
						entryTuple.getT3()
				), Tuples.of(DbType.HASH_MAP, entryTuple.getT1(),
						entryTuple.getT2(),
						false
				)))
				.filter(tuple -> !(tuple.getT1() == DbType.HASH_MAP && tuple.getT2() != UpdateMode.ALLOW))
				.map(fullTuple -> Arguments.of(fullTuple.getT1(), fullTuple.getT2(), fullTuple.getT3(), fullTuple.getT4()));
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsPutMulti")
	public void testPutMultiGetMulti(DbType dbType, UpdateMode updateMode, Map<String, String> entries, boolean shouldFail) {
		var remainingEntries = new ConcurrentHashMap<Entry<String, String>, Boolean>().keySet(true);
		Step<Entry<String, String>> stpVer = StepVerifier
				.create(tempDb(allocator, db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryMap(dict, dbType, 5))
						.flatMapMany(map -> Flux
								.concat(
										map.putMulti(Flux.fromIterable(entries.entrySet())).then(Mono.empty()),
										map.getMulti(null, Flux.fromIterable(entries.keySet()))
								)
								.doAfterTerminate(map::release)
						)
						.filter(k -> k.getValue().isPresent())
						.map(k -> Map.entry(k.getKey(), k.getValue().orElseThrow()))
						.transform(LLUtils::handleDiscard)
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
	public void testSetAllValuesGetMulti(DbType dbType, UpdateMode updateMode, Map<String, String> entries, boolean shouldFail) {
		var remainingEntries = new ConcurrentHashMap<Entry<String, String>, Boolean>().keySet(true);
		Step<Entry<String, String>> stpVer = StepVerifier
				.create(tempDb(allocator, db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryMap(dict, dbType, 5))
						.flatMapMany(map -> map
								.setAllValues(Flux.fromIterable(entries.entrySet()))
								.thenMany(map.getMulti(null, Flux.fromIterable(entries.keySet())))
								.doAfterTerminate(map::release)
						)
						.filter(k -> k.getValue().isPresent())
						.map(k -> Map.entry(k.getKey(), k.getValue().orElseThrow()))
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
	public void testSetAllValuesAndGetPrevious(DbType dbType, UpdateMode updateMode, Map<String, String> entries, boolean shouldFail) {
		var remainingEntries = new ConcurrentHashMap<Entry<String, String>, Boolean>().keySet(true);
		Step<Entry<String, String>> stpVer = StepVerifier
				.create(tempDb(allocator, db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryMap(dict, dbType, 5))
						.flatMapMany(map -> Flux
								.concat(
										map.setAllValuesAndGetPrevious(Flux.fromIterable(entries.entrySet())),
										map.setAllValuesAndGetPrevious(Flux.fromIterable(entries.entrySet()))
								)
								.doAfterTerminate(map::release)
						)
						.transform(LLUtils::handleDiscard)
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
	public void testSetGetMulti(DbType dbType, UpdateMode updateMode, Map<String, String> entries, boolean shouldFail) {
		var remainingEntries = new ConcurrentHashMap<Entry<String, String>, Boolean>().keySet(true);
		Step<Entry<String, String>> stpVer = StepVerifier
				.create(tempDb(allocator, db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryMap(dict, dbType, 5))
						.flatMapMany(map -> Flux
								.concat(
										map.set(entries).then(Mono.empty()),
										map.getMulti(null, Flux.fromIterable(entries.keySet()))
								)
								.doAfterTerminate(map::release)
						)
						.filter(k -> k.getValue().isPresent())
						.map(k -> Map.entry(k.getKey(), k.getValue().orElseThrow()))
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
	public void testSetAndGetChanged(DbType dbType,
			UpdateMode updateMode,
			Map<String, String> entries,
			boolean shouldFail) {
		Step<Boolean> stpVer = StepVerifier
				.create(tempDb(allocator, db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryMap(dict, dbType, 5))
						.flatMapMany(map -> {
							Mono<Void> removalMono;
							if (entries.isEmpty()) {
								removalMono = Mono.empty();
							} else {
								removalMono = map.remove(entries.keySet().stream().findFirst().orElseThrow());
							}
							return Flux
									.concat(
											map.setAndGetChanged(entries).single(),
											map.setAndGetChanged(entries).single(),
											removalMono.then(Mono.empty()),
											map.setAndGetChanged(entries).single()
									)
									.doAfterTerminate(map::release);
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
	public void testSetAndGetPrevious(DbType dbType, UpdateMode updateMode, Map<String, String> entries, boolean shouldFail) {
		var remainingEntries = new ConcurrentHashMap<Entry<String, String>, Boolean>().keySet(true);
		Step<Entry<String, String>> stpVer = StepVerifier
				.create(tempDb(allocator, db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryMap(dict, dbType, 5))
						.flatMapMany(map -> Flux
								.concat(map.setAndGetPrevious(entries), map.setAndGetPrevious(entries))
								.map(Map::entrySet)
								.concatMapIterable(list -> list)
								.doAfterTerminate(map::release)
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
	public void testSetClearAndGetPreviousGet(DbType dbType, UpdateMode updateMode, Map<String, String> entries, boolean shouldFail) {
		var remainingEntries = new ConcurrentHashMap<Entry<String, String>, Boolean>().keySet(true);
		Step<Entry<String, String>> stpVer = StepVerifier
				.create(tempDb(allocator, db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryMap(dict, dbType, 5))
						.flatMapMany(map -> Flux
								.concat(map.set(entries).then(Mono.empty()), map.clearAndGetPrevious(), map.get(null))
								.map(Map::entrySet)
								.concatMapIterable(list -> list)
								.doAfterTerminate(map::release)
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
	public void testPutMultiGetAllValues(DbType dbType, UpdateMode updateMode, Map<String, String> entries, boolean shouldFail) {
		var remainingEntries = new ConcurrentHashMap<Entry<String, String>, Boolean>().keySet(true);
		Step<Entry<String, String>> stpVer = StepVerifier
				.create(tempDb(allocator, db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryMap(dict, dbType, 5))
						.flatMapMany(map -> Flux
								.concat(
										map.putMulti(Flux.fromIterable(entries.entrySet())).then(Mono.empty()),
										map.getAllValues(null)
								)
								.doAfterTerminate(map::release)
						)
						.transform(LLUtils::handleDiscard)
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
	public void testPutMultiGet(DbType dbType, UpdateMode updateMode, Map<String, String> entries, boolean shouldFail) {
		var remainingEntries = new ConcurrentHashMap<Entry<String, String>, Boolean>().keySet(true);
		Step<Entry<String, String>> stpVer = StepVerifier
				.create(tempDb(allocator, db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryMap(dict, dbType, 5))
						.flatMapMany(map -> Flux
								.concat(
										map.putMulti(Flux.fromIterable(entries.entrySet())).then(Mono.empty()),
										map.get(null)
										.map(Map::entrySet)
										.flatMapIterable(list -> list)
								)
								.doAfterTerminate(map::release)
						)
						.transform(LLUtils::handleDiscard)
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
	public void testPutMultiGetAllStagesGet(DbType dbType, UpdateMode updateMode, Map<String, String> entries, boolean shouldFail) {
		var remainingEntries = new ConcurrentHashMap<Entry<String, String>, Boolean>().keySet(true);
		Step<Entry<String, String>> stpVer = StepVerifier
				.create(tempDb(allocator, db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryMap(dict, dbType, 5))
						.flatMapMany(map -> Flux
								.concat(
										map.putMulti(Flux.fromIterable(entries.entrySet())).then(Mono.empty()),
										map
												.getAllStages(null)
												.flatMap(stage -> stage
														.getValue()
														.get(null)
														.map(val -> Map.entry(stage.getKey(), val))
														.doAfterTerminate(() -> stage.getValue().release())
												)
								)
								.doAfterTerminate(map::release)
						)
						.transform(LLUtils::handleDiscard)
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
	public void testPutMultiIsEmpty(DbType dbType, UpdateMode updateMode, Map<String, String> entries, boolean shouldFail) {
		var remainingEntries = new ConcurrentHashMap<Entry<String, String>, Boolean>().keySet(true);
		Step<Boolean> stpVer = StepVerifier
				.create(tempDb(allocator, db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryMap(dict, dbType, 5))
						.flatMapMany(map -> Flux
								.concat(
										map.isEmpty(null),
										map.putMulti(Flux.fromIterable(entries.entrySet())).then(Mono.empty()),
										map.isEmpty(null)
								)
								.doAfterTerminate(map::release)
						)
						.flatMap(val -> shouldFail ? Mono.empty() : Mono.just(val))
						.transform(LLUtils::handleDiscard)
				));
		if (shouldFail) {
			stpVer.verifyError();
		} else {
			stpVer.expectNext(true, entries.isEmpty()).verifyComplete();
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsPutMulti")
	public void testPutMultiClear(DbType dbType, UpdateMode updateMode, Map<String, String> entries, boolean shouldFail) {
		Step<Boolean> stpVer = StepVerifier
				.create(tempDb(allocator, db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryMap(dict, dbType, 5))
						.flatMapMany(map -> Flux
								.concat(
										map.isEmpty(null),
										map.putMulti(Flux.fromIterable(entries.entrySet())).then(Mono.empty()),
										map.isEmpty(null),
										map.clear().then(Mono.empty()),
										map.isEmpty(null)
								)
								.doAfterTerminate(map::release)
						)
						.flatMap(val -> shouldFail ? Mono.empty() : Mono.just(val))
						.transform(LLUtils::handleDiscard)
				));
		if (shouldFail) {
			stpVer.verifyError();
		} else {
			stpVer.expectNext(true, entries.isEmpty(), true).verifyComplete();
		}
	}
}
