package it.cavallium.dbengine.tests;

import static it.cavallium.dbengine.tests.DbTestUtils.*;

import it.cavallium.dbengine.database.UpdateMode;
import it.unimi.dsi.fastutil.objects.Object2ObjectSortedMaps;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public abstract class TestDictionaryMap {

	private static final Logger log = LogManager.getLogger(TestDictionaryMap.class);
	private boolean checkLeaks = true;

	private static boolean isTestBadKeysEnabled() {
		return !isCIMode() && System.getProperty("badkeys", "true").equalsIgnoreCase("true");
	}

	protected abstract TemporaryDbGenerator getTempDbGenerator();

	record Tuple2<X, Y>(X getT1, Y getT2) {}
	record Tuple3<X, Y, Z>(X getT1, Y getT2, Z getT3) {}
	record Tuple4<X, Y, Z, W>(X getT1, Y getT2, Z getT3, W getT4) {}
	record Tuple5<X, Y, Z, W, X1>(X getT1, Y getT2, Z getT3, W getT4, X1 getT5) {}

	private static Stream<Arguments> provideArgumentsPut() {
		var goodKeys = List.of("12345");
		List<String> badKeys;
		if (isTestBadKeysEnabled()) {
			badKeys = List.of("", "aaaa", "aaaaaa");
		} else {
			badKeys = List.of();
		}
		List<Tuple2<String, Boolean>> keys = Stream
				.concat(goodKeys.stream().map(s -> new Tuple2<>(s, false)), badKeys.stream().map(s -> new Tuple2<>(s, true)))
				.toList();
		var values = isCIMode() ? List.of("val") : List.of("", "\0", BIG_STRING);

		return keys
				.stream()
				.flatMap(keyTuple -> {
					Stream<String> strm;
					if (keyTuple.getT2()) {
						strm = values.stream().findFirst().stream();
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
				.flatMap(entryTuple -> Stream.of(new Tuple5<>(MapType.MAP, entryTuple.getT1(),
						entryTuple.getT2(),
						entryTuple.getT3(),
						entryTuple.getT4()
				), new Tuple5<>(MapType.HASH_MAP, entryTuple.getT1(),
						entryTuple.getT2(),
						entryTuple.getT3(),
						false
				)))
				.filter(tuple -> !(tuple.getT1() == MapType.HASH_MAP && tuple.getT2() != UpdateMode.ALLOW))
				.map(fullTuple -> Arguments.of(fullTuple.getT1(), fullTuple.getT2(), fullTuple.getT3(), fullTuple.getT4(), fullTuple.getT5()));
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
	@MethodSource("provideArgumentsPut")
	public void testPut(MapType mapType, UpdateMode updateMode, String key, String value, boolean shouldFail)
			throws IOException {
		var gen = getTempDbGenerator();
		var db = gen.openTempDb();
		var dict = tempDictionary(db.db(), updateMode);
		var map = tempDatabaseMapDictionaryMap(dict, mapType, 5);

		runVoid(shouldFail, () -> map.putValue(key, value));

		var resultingMapSize = map.leavesCount(null, false);
		Assertions.assertEquals(shouldFail ? 0 : 1, resultingMapSize);

		var resultingMap = map.get(null);
		Assertions.assertEquals(shouldFail ? null : Object2ObjectSortedMaps.singleton(key, value), resultingMap);

		//if (shouldFail) this.checkLeaks = false;

		gen.closeTempDb(db);
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsPut")
	public void testAtSetAtGet(MapType mapType, UpdateMode updateMode, String key, String value, boolean shouldFail)
			throws IOException {
		var result = tempDb(getTempDbGenerator(), db -> {
			var map = tempDatabaseMapDictionaryMap(tempDictionary(db, updateMode), mapType, 5);
			return run(shouldFail, () -> {
				map.at(null, key).set(value);
				return map.at(null, key).get(null);
			});
		});
		if (shouldFail) {
			this.checkLeaks = false;
		} else {
			Assertions.assertEquals(value, result);
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsPut")
	public void testPutAndGetPrevious(MapType mapType, UpdateMode updateMode, String key, String value, boolean shouldFail)
			throws IOException {
		var result = tempDb(getTempDbGenerator(), db -> {
			var map = tempDatabaseMapDictionaryMap(tempDictionary(db, updateMode), mapType, 5);
			return run(shouldFail,
					() -> Arrays.asList(map.putValueAndGetPrevious(key, "error?"),
							map.putValueAndGetPrevious(key, value),
							map.putValueAndGetPrevious(key, value)
					)
			);
		});
		if (shouldFail) {
			this.checkLeaks = false;
		} else {
			Assertions.assertArrayEquals(new String[] {null, "error?", value}, result.toArray(String[]::new));
		}
	}

	/*
	@ParameterizedTest
	@MethodSource("provideArgumentsPut")
	public void testPutValueRemoveAndGetPrevious(MapType mapType, UpdateMode updateMode, String key, String value, boolean shouldFail) {
		var stpVer = StepVerifier
				.create(tempDb(getTempDbGenerator(), allocator, db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryMap(dict, mapType, 5))
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
	public void testPutValueRemoveAndGetStatus(MapType mapType, UpdateMode updateMode, String key, String value, boolean shouldFail) {
		var stpVer = StepVerifier
				.create(tempDb(getTempDbGenerator(), allocator, db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryMap(dict, mapType, 5))
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
	public void testUpdate(MapType mapType, UpdateMode updateMode, String key, String value, boolean shouldFail) {
		if (updateMode == UpdateMode.DISALLOW && !isTestBadKeysEnabled()) {
			return;
		}
		var stpVer = StepVerifier
				.create(tempDb(getTempDbGenerator(), allocator, db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryMap(dict, mapType, 5))
						.flatMapMany(map -> Flux
								.concat(
										map.updateValue(key, old -> {
											Assertions.assertNull(old);
											return "error?";
										}),
										map.updateValue(key, old -> {
											Assertions.assertEquals("error?", old);
											return "error?";
										}),
										map.updateValue(key, old -> {
											Assertions.assertEquals("error?", old);
											return "error?";
										}),
										map.updateValue(key, old -> {
											Assertions.assertEquals("error?", old);
											return value;
										}),
										map.updateValue(key, old -> {
											Assertions.assertEquals(value, old);
											return value;
										})
								)
								.doFinally(s -> map.close())
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
	public void testUpdateGet(MapType mapType, UpdateMode updateMode, String key, String value, boolean shouldFail) {
		if (updateMode == UpdateMode.DISALLOW && !isTestBadKeysEnabled()) {
			return;
		}
		var stpVer = StepVerifier
				.create(tempDb(getTempDbGenerator(), allocator, db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryMap(dict, mapType, 5))
						.flatMapMany(map -> Flux
								.concat(
										Mono
												.fromRunnable(() -> log.debug("1. Updating value: {}", key))
												.then(map.updateValue(key, old -> {
													assert old == null;
													return "error?";
												}))
												.doOnSuccess(s -> log.debug("1. Getting value: {}", key))
												.then(map.getValue(null, key)),

										Mono
												.fromRunnable(() -> log.debug("2. Updating value: {}", key))
												.then(map.updateValue(key, old -> {
													assert Objects.equals(old, "error?");
													return "error?";
												}))
												.doOnSuccess(s -> log.debug("2. Getting value: {}", key))
												.then(map.getValue(null, key)),

										Mono
												.fromRunnable(() -> log.debug("3. Updating value: {}", key))
												.then(map.updateValue(key, old -> {
													assert Objects.equals(old, "error?");
													return "error?";
												}))
												.doOnSuccess(s -> log.debug("3. Getting value: {}", key))
												.then(map.getValue(null, key)),

										Mono
												.fromRunnable(() -> log.debug("4. Updating value: {}", key))
												.then(map.updateValue(key, old -> {
													assert Objects.equals(old, "error?");
													return value;
												}))
												.doOnSuccess(s -> log.debug("4. Getting value: {}", key))
												.then(map.getValue(null, key)),

										Mono
												.fromRunnable(() -> log.debug("5. Updating value: {}", key))
												.then(map.updateValue(key, old -> {
													assert Objects.equals(old, value);
													return value;
												}))
												.doOnSuccess(s -> log.debug("5. Getting value: {}", key))
												.then(map.getValue(null, key))
								)
								.doFinally(s -> map.close())
						)
				));
		if (updateMode == UpdateMode.DISALLOW || shouldFail) {
			stpVer.verifyError();
		} else {
			stpVer.expectNext("error?", "error?", "error?", value, value).verifyComplete();
		}
	}

	@Test
	public void testUpdateGetWithCancel() {
		tempDb(getTempDbGenerator(), allocator, db -> {
			var mapMono = tempDictionary(db, UpdateMode.ALLOW)
					.map(dict -> tempDatabaseMapDictionaryMap(dict, MapType.MAP, 5));

			var keys = Flux.range(10, 89).map(n -> "key" + n).repeat(100);

			return Mono.usingWhen(mapMono,
					map -> keys.flatMap(key -> map.updateValue(key, prevValue -> key + "-val")).then(),
					LLUtils::finalizeResource
			);
		}).take(50).blockLast();
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsPut")
	public void testPutAndGetChanged(MapType mapType, UpdateMode updateMode, String key, String value, boolean shouldFail) {
		var stpVer = StepVerifier
				.create(tempDb(getTempDbGenerator(), allocator, db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryMap(dict, mapType, 5))
						.flatMapMany(map -> Flux
								.concat(
										map.putValueAndGetChanged(key, "error?").single(),
										map.putValueAndGetChanged(key, value).single(),
										map.putValueAndGetChanged(key, value).single(),
										map.remove(key),
										map.putValueAndGetChanged(key, "error?").single()
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

	private static Stream<Arguments> provideArgumentsPutMulti() {
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
		var values = isCIMode() ? List.of("val") : List.of("", "\0", BIG_STRING);

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
				.flatMap(entryTuple -> Stream.of(new Tuple2<>(MapType.MAP, entryTuple.getT1(),
						entryTuple.getT2(),
						entryTuple.getT3()
				), new Tuple2<>(MapType.HASH_MAP, entryTuple.getT1(),
						entryTuple.getT2(),
						false
				)))
				.filter(tuple -> !(tuple.getT1() == MapType.HASH_MAP && tuple.getT2() != UpdateMode.ALLOW))
				.map(fullTuple -> Arguments.of(fullTuple.getT1(), fullTuple.getT2(), fullTuple.getT3(), fullTuple.getT4()));
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsPutMulti")
	public void testPutMultiGetMulti(MapType mapType, UpdateMode updateMode, Object2ObjectSortedMap<String, String> entries, boolean shouldFail) {
		var remainingEntries = new ConcurrentHashMap<Entry<String, String>, Boolean>().keySet(true);
		Step<Entry<String, String>> stpVer = StepVerifier
				.create(tempDb(getTempDbGenerator(), allocator, db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryMap(dict, mapType, 5))
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

						.filter(entry -> entry.getValue().isPresent())
						.map(k -> Map.entry(k.getKey(), k.getValue().orElseThrow()))
				));
		if (shouldFail) {
			this.checkLeaks = false;
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
	public void testSetAllValuesGetMulti(MapType mapType, UpdateMode updateMode, Object2ObjectSortedMap<String, String> entries, boolean shouldFail) {
		var remainingEntries = new ConcurrentHashMap<Entry<String, String>, Boolean>().keySet(true);
		Step<Entry<String, String>> stpVer = StepVerifier
				.create(tempDb(getTempDbGenerator(), allocator, db -> {
					var mapMono = tempDictionary(db, updateMode).map(dict -> tempDatabaseMapDictionaryMap(dict, mapType, 5));
							return Flux.usingWhen(mapMono, map -> {
										Flux<Entry<String, String>> entriesFlux = Flux.fromIterable(entries.entrySet());
										Flux<String> keysFlux = entriesFlux.map(Entry::getKey);
										Flux<Optional<String>> resultsFlux = map.setAllValues(entriesFlux).thenMany(map.getMulti(null, keysFlux));
										return Flux.zip(keysFlux, resultsFlux, Map::entry);
									}, LLUtils::finalizeResource)
									.filter(k -> k.getValue().isPresent()).map(k -> Map.entry(k.getKey(), k.getValue().orElseThrow()));
						}
				));
		if (shouldFail) {
			this.checkLeaks = false;
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
	public void testSetAllValuesAndGetPrevious(MapType mapType, UpdateMode updateMode, Object2ObjectSortedMap<String, String> entries, boolean shouldFail) {
		var remainingEntries = new ConcurrentHashMap<Entry<String, String>, Boolean>().keySet(true);
		Step<Entry<String, String>> stpVer = StepVerifier
				.create(tempDb(getTempDbGenerator(), allocator, db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryMap(dict, mapType, 5))
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
			for (Entry<String, String> ignored : remainingEntries) {
				stpVer = stpVer.expectNextMatches(remainingEntries::remove);
			}
			stpVer.verifyComplete();
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsPutMulti")
	public void testSetGetMulti(MapType mapType,
			UpdateMode updateMode,
			Object2ObjectSortedMap<String, String> entries,
			boolean shouldFail) {
		var remainingEntries = new ConcurrentHashMap<Entry<String, String>, Boolean>().keySet(true);
		Step<Entry<String, String>> stpVer = StepVerifier
				.create(tempDb(getTempDbGenerator(), allocator, db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryMap(dict, mapType, 5))
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
			for (Entry<String, String> ignored : remainingEntries) {
				stpVer = stpVer.expectNextMatches(remainingEntries::remove);
			}
			stpVer.verifyComplete();
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsPutMulti")
	public void testSetAndGetChanged(MapType mapType,
			UpdateMode updateMode,
			Object2ObjectSortedMap<String, String> entries,
			boolean shouldFail) {
		Step<Boolean> stpVer = StepVerifier
				.create(tempDb(getTempDbGenerator(), allocator, db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryMap(dict, mapType, 5))
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
	@MethodSource("provideArgumentsPutMulti")
	public void testSetAndGetPrevious(MapType mapType,
			UpdateMode updateMode,
			Object2ObjectSortedMap<String, String> entries,
			boolean shouldFail) {
		var remainingEntries = new ConcurrentHashMap<Entry<String, String>, Boolean>().keySet(true);
		Step<Entry<String, String>> stpVer = StepVerifier
				.create(tempDb(getTempDbGenerator(), allocator, db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryMap(dict, mapType, 5))
						.flatMapMany(map -> Flux
								.concat(map.setAndGetPrevious(entries), map.setAndGetPrevious(entries))
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
			for (Entry<String, String> ignored : remainingEntries) {
				stpVer = stpVer.expectNextMatches(remainingEntries::remove);
			}
			stpVer.verifyComplete();
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsPutMulti")
	public void testSetClearAndGetPreviousGet(MapType mapType,
			UpdateMode updateMode,
			Object2ObjectSortedMap<String, String> entries,
			boolean shouldFail) {
		var remainingEntries = new ConcurrentHashMap<Entry<String, String>, Boolean>().keySet(true);
		Step<Entry<String, String>> stpVer = StepVerifier
				.create(tempDb(getTempDbGenerator(), allocator, db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryMap(dict, mapType, 5))
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
			for (Entry<String, String> ignored : remainingEntries) {
				stpVer = stpVer.expectNextMatches(remainingEntries::remove);
			}
			stpVer.verifyComplete();
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsPutMulti")
	public void testPutMultiGetAllValues(MapType mapType, UpdateMode updateMode, Object2ObjectSortedMap<String, String> entries, boolean shouldFail) {
		var remainingEntries = new ConcurrentHashMap<Entry<String, String>, Boolean>().keySet(true);
		Step<Entry<String, String>> stpVer = StepVerifier
				.create(tempDb(getTempDbGenerator(), allocator, db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryMap(dict, mapType, 5))
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
			for (Entry<String, String> ignored : remainingEntries) {
				stpVer = stpVer.expectNextMatches(remainingEntries::remove);
			}
			stpVer.verifyComplete();
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsPutMulti")
	public void testPutMultiGet(MapType mapType, UpdateMode updateMode, Object2ObjectSortedMap<String, String> entries, boolean shouldFail) {
		var remainingEntries = new ConcurrentHashMap<Entry<String, String>, Boolean>().keySet(true);
		Step<Entry<String, String>> stpVer = StepVerifier
				.create(tempDb(getTempDbGenerator(), allocator, db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryMap(dict, mapType, 5))
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
			for (Entry<String, String> ignored : remainingEntries) {
				stpVer = stpVer.expectNextMatches(remainingEntries::remove);
			}
			stpVer.verifyComplete();
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsPutMulti")
	public void testPutMultiGetAllStagesGet(MapType mapType, UpdateMode updateMode, Object2ObjectSortedMap<String, String> entries, boolean shouldFail) {
		var remainingEntries = new ConcurrentHashMap<Entry<String, String>, Boolean>().keySet(true);
		Step<Entry<String, String>> stpVer = StepVerifier
				.create(tempDb(getTempDbGenerator(), allocator, db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryMap(dict, mapType, 5))
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
			for (Entry<String, String> ignored : remainingEntries) {
				stpVer = stpVer.expectNextMatches(remainingEntries::remove);
			}
			stpVer.verifyComplete();
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsPutMulti")
	public void testPutMultiIsEmpty(MapType mapType, UpdateMode updateMode, Object2ObjectSortedMap<String, String> entries, boolean shouldFail) {
		var remainingEntries = new ConcurrentHashMap<Entry<String, String>, Boolean>().keySet(true);
		Step<Boolean> stpVer = StepVerifier
				.create(tempDb(getTempDbGenerator(), allocator, db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryMap(dict, mapType, 5))
						.flatMapMany(map -> Flux
								.concat(
										map.isEmpty(null),
										map.putMulti(Flux.fromIterable(entries.entrySet())).then(Mono.empty()),
										map.isEmpty(null)
								)
								.doFinally(s -> map.close())
						)
						.flatMap(val -> shouldFail ? Mono.empty() : Mono.just(val))
				));
		if (shouldFail) {
			this.checkLeaks = false;
			stpVer.verifyError();
		} else {
			stpVer.expectNext(true, entries.isEmpty()).verifyComplete();
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsPutMulti")
	public void testPutMultiClear(MapType mapType, UpdateMode updateMode, Object2ObjectSortedMap<String, String> entries, boolean shouldFail) {
		List<Boolean> result;
		try {
			result = SyncUtils.run(DbTestUtils.tempDb(getTempDbGenerator(), allocator, db -> tempDictionary(db, updateMode)
					.map(dict -> tempDatabaseMapDictionaryMap(dict, mapType, 5))
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
					.flatMap(val -> shouldFail ? Mono.empty() : Mono.just(val))
					.collectList()
			).singleOrEmpty());
		} catch (Exception ex) {
			if (shouldFail) {
				this.checkLeaks = false;
			} else {
				throw ex;
			}
			return;
		}

		Assertions.assertEquals(true, result.get(0));

		Assertions.assertEquals(entries.isEmpty(), result.get(1));

		Assertions.assertEquals(true, result.get(2));
	}
	 */
}
