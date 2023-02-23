package it.cavallium.dbengine.tests;

import static it.cavallium.dbengine.tests.DbTestUtils.*;
import static it.cavallium.dbengine.utils.StreamUtils.toListClose;

import com.google.common.collect.Streams;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.UpdateMode;
import it.cavallium.dbengine.utils.StreamUtils;
import it.unimi.dsi.fastutil.objects.Object2ObjectSortedMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectSortedMaps;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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

	@ParameterizedTest
	@MethodSource("provideArgumentsPut")
	public void testPutValueRemoveAndGetPrevious(MapType mapType, UpdateMode updateMode, String key, String value, boolean shouldFail) {
		var stpVer = run(shouldFail, () -> tempDb(getTempDbGenerator(), db -> {
			var map = tempDatabaseMapDictionaryMap(tempDictionary(db, updateMode), mapType, 5);
			var x1 = map.removeAndGetPrevious(key);
			map.putValue(key, value);
			var x2 = map.removeAndGetPrevious(key);
			var x3 = map.removeAndGetPrevious(key);
			return Arrays.asList(x1, x2, x3);
		}));
		if (shouldFail) {
			this.checkLeaks = false;
		} else {
			Assertions.assertEquals(Arrays.asList(null, value, null), stpVer);
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsPut")
	public void testPutValueRemoveAndGetStatus(MapType mapType, UpdateMode updateMode, String key, String value, boolean shouldFail) {
		var stpVer = run(shouldFail, () -> tempDb(getTempDbGenerator(), db -> {
			var map = tempDatabaseMapDictionaryMap(tempDictionary(db, updateMode), mapType, 5);
					var x1 = map.removeAndGetStatus(key);
					map.putValue(key, value);
					var x2 = map.removeAndGetStatus(key);
					var x3 = map.removeAndGetStatus(key);
					return Stream.of(x1, x2, x3).toList();
				}));
		if (shouldFail) {
			this.checkLeaks = false;
		} else {
			Assertions.assertEquals(Arrays.asList(false, true, false), stpVer);
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsPut")
	public void testUpdate(MapType mapType, UpdateMode updateMode, String key, String value, boolean shouldFail) {
		if (updateMode == UpdateMode.DISALLOW && !isTestBadKeysEnabled()) {
			return;
		}
		var stpVer = run(updateMode == UpdateMode.DISALLOW || shouldFail, () -> tempDb(getTempDbGenerator(), db -> {
			var map = tempDatabaseMapDictionaryMap(tempDictionary(db, updateMode), mapType, 5);
			return Arrays.asList(map.updateValue(key, old -> {
				Assertions.assertNull(old);
				return "error?";
			}), map.updateValue(key, old -> {
				Assertions.assertEquals("error?", old);
				return "error?";
			}), map.updateValue(key, old -> {
				Assertions.assertEquals("error?", old);
				return "error?";
			}), map.updateValue(key, old -> {
				Assertions.assertEquals("error?", old);
				return value;
			}), map.updateValue(key, old -> {
				Assertions.assertEquals(value, old);
				return value;
			}));
		}));
		if (updateMode != UpdateMode.DISALLOW && !shouldFail) {
			Assertions.assertEquals(Arrays.asList(true, false, false, true, false), stpVer);
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsPut")
	public void testUpdateGet(MapType mapType, UpdateMode updateMode, String key, String value, boolean shouldFail) {
		if (updateMode == UpdateMode.DISALLOW && !isTestBadKeysEnabled()) {
			return;
		}
		var stpVer = run(updateMode == UpdateMode.DISALLOW || shouldFail, () -> tempDb(getTempDbGenerator(), db -> {
			var map = tempDatabaseMapDictionaryMap(tempDictionary(db, updateMode), mapType, 5);
			log.debug("1. Updating value: {}", key);
			map.updateValue(key, old -> {
				assert old == null;
				return "error?";
			});
			log.debug("1. Getting value: {}", key);
			var x2 = map.getValue(null, key);

			log.debug("2. Updating value: {}", key);
			map.updateValue(key, old -> {
				assert Objects.equals(old, "error?");
				return "error?";
			});
			log.debug("2. Getting value: {}", key);
			var x3 = map.getValue(null, key);

			log.debug("3. Updating value: {}", key);
			map.updateValue(key, old -> {
				assert Objects.equals(old, "error?");
				return "error?";
			});
			log.debug("3. Getting value: {}", key);
			var x4 = map.getValue(null, key);

			log.debug("4. Updating value: {}", key);
			map.updateValue(key, old -> {
				assert Objects.equals(old, "error?");
				return value;
			});
			log.debug("4. Getting value: {}", key);
			var x5 = map.getValue(null, key);

			log.debug("5. Updating value: {}", key);
			map.updateValue(key, old -> {
				assert Objects.equals(old, value);
				return value;
			});
			log.debug("5. Getting value: {}", key);
			var x6 = map.getValue(null, key);
			return Arrays.asList(x2, x3, x4, x5, x6);
		}));
		if (updateMode != UpdateMode.DISALLOW && !shouldFail) {
			Assertions.assertEquals(Arrays.asList("error?", "error?", "error?", value, value), stpVer);
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsPut")
	public void testPutAndGetChanged(MapType mapType, UpdateMode updateMode, String key, String value, boolean shouldFail) {
		var stpVer = run(shouldFail, () -> tempDb(getTempDbGenerator(), db -> {
			var map = tempDatabaseMapDictionaryMap(tempDictionary(db, updateMode), mapType, 5);
			var x1 = map.putValueAndGetChanged(key, "error?");
			var x2 = map.putValueAndGetChanged(key, value);
			var x3 = map.putValueAndGetChanged(key, value);
			map.remove(key);
			var x4 = map.putValueAndGetChanged(key, "error?");
			return Arrays.asList(x1, x2, x3, x4);
		}));
		if (shouldFail) {
			this.checkLeaks = false;
		} else {
			Assertions.assertEquals(Arrays.asList(true, true, false, true), stpVer);
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
				.map(keyTuple -> new Tuple2<>(Streams
						.zip(keyTuple.getT1().stream(), values.stream(), Tuple2::new)
						.collect(Collectors.toMap(Tuple2::getT1,
								Tuple2::getT2,
								(a, b) -> a,
								it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap::new
						)), keyTuple.getT2()))
				.flatMap(entryTuple -> Arrays
						.stream(UpdateMode.values())
						.map(updateMode -> new Tuple3<>(updateMode, entryTuple.getT1(), entryTuple.getT2())))
				.flatMap(entryTuple -> Stream.of(new Tuple4<>(MapType.MAP,
						entryTuple.getT1(),
						entryTuple.getT2(),
						entryTuple.getT3()
				), new Tuple4<>(MapType.HASH_MAP, entryTuple.getT1(), entryTuple.getT2(), false)))
				.filter(tuple -> !(tuple.getT1() == MapType.HASH_MAP && tuple.getT2() != UpdateMode.ALLOW))
				.map(fullTuple -> Arguments.of(fullTuple.getT1(), fullTuple.getT2(), fullTuple.getT3(), fullTuple.getT4()));
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsPutMulti")
	public void testPutMultiGetMulti(MapType mapType, UpdateMode updateMode, Object2ObjectSortedMap<String, String> entries, boolean shouldFail) {
		var remainingEntries = new ArrayList<Entry<String, String>>();
		var stpVer = run(shouldFail, () -> tempDb(getTempDbGenerator(), db -> {
			var map = tempDatabaseMapDictionaryMap(tempDictionary(db, updateMode), mapType, 5);

			var entriesFlux = entries.entrySet();
			var keysFlux = entriesFlux.stream().map(Entry::getKey).toList();
			map.putMulti(entriesFlux.stream());
			List<Optional<String>> results;
			try (var resultsFlux = map.getMulti(null, keysFlux.stream())) {
				results = resultsFlux.toList();
			}
			return Streams
					.zip(keysFlux.stream(), results.stream(), Map::entry)
					.filter(entry -> entry.getValue().isPresent())
					.map(k -> Map.entry(k.getKey(), k.getValue().orElseThrow()))
					.toList();
		}));
		if (shouldFail) {
			this.checkLeaks = false;
		} else {
			entries.forEach((k, v) -> remainingEntries.add(Map.entry(k, v)));
			Assertions.assertEquals(remainingEntries, stpVer);
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsPutMulti")
	public void testSetAllValuesGetMulti(MapType mapType, UpdateMode updateMode, Object2ObjectSortedMap<String, String> entries, boolean shouldFail) {
		var remainingEntries = new ArrayList<Entry<String, String>>();
		var stpVer = run(shouldFail, () -> tempDb(getTempDbGenerator(), db -> {
			var map = tempDatabaseMapDictionaryMap(tempDictionary(db, updateMode), mapType, 5);

			var entriesFlux = entries.entrySet();
			var keysFlux = entriesFlux.stream().map(Entry::getKey).toList();
			map.setAllValues(entriesFlux.stream());
			List<Optional<String>> resultsFlux;
			try (var stream = map.getMulti(null, keysFlux.stream())) {
				resultsFlux = stream.toList();
			}
			return Streams
					.zip(keysFlux.stream(), resultsFlux.stream(), Map::entry)
					.filter(k -> k.getValue().isPresent())
					.map(k -> Map.entry(k.getKey(), k.getValue().orElseThrow()))
					.toList();
		}));
		if (shouldFail) {
			this.checkLeaks = false;
		} else {
			entries.forEach((k, v) -> remainingEntries.add(Map.entry(k, v)));
			Assertions.assertEquals(remainingEntries, stpVer);
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsPutMulti")
	public void testSetAllValuesAndGetPrevious(MapType mapType,
			UpdateMode updateMode,
			Object2ObjectSortedMap<String, String> entries,
			boolean shouldFail) {
		var remainingEntries = new ArrayList<Entry<String, String>>();
		var stpVer = run(shouldFail, () -> tempDb(getTempDbGenerator(), db -> {
			var map = tempDatabaseMapDictionaryMap(tempDictionary(db, updateMode), mapType, 5);
			return Arrays.asList(toListClose(map.setAllValuesAndGetPrevious(entries.entrySet().stream())),
					toListClose(map.setAllValuesAndGetPrevious(entries.entrySet().stream()))
			);
		}));
		if (shouldFail) {
			this.checkLeaks = false;
		} else {
			entries.forEach((k, v) -> remainingEntries.add(Map.entry(k, v)));
			Assertions.assertEquals(Arrays.asList(List.of(), remainingEntries), stpVer);
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsPutMulti")
	public void testSetGetMulti(MapType mapType,
			UpdateMode updateMode,
			Object2ObjectSortedMap<String, String> entries,
			boolean shouldFail) {
		var remainingEntries = new ArrayList<Entry<String, String>>();
		var stpVer = run(shouldFail, () -> tempDb(getTempDbGenerator(), db -> {
			var map = tempDatabaseMapDictionaryMap(tempDictionary(db, updateMode), mapType, 5);
			var entriesFlux = entries.entrySet();
			var keysFlux = entriesFlux.stream().map(Entry::getKey).toList();
			map.set(entries);
			var resultsFlux = toListClose(map.getMulti(null, entries.keySet().stream()));
			return Streams
					.zip(keysFlux.stream(), resultsFlux.stream(), Map::entry)
					.filter(k -> k.getValue().isPresent())
					.map(k -> Map.entry(k.getKey(), k.getValue().orElseThrow()))
					.toList();
		}));
		if (shouldFail) {
			this.checkLeaks = false;
		} else {
			entries.forEach((k, v) -> remainingEntries.add(Map.entry(k, v)));
			Assertions.assertEquals(remainingEntries, stpVer);
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsPutMulti")
	public void testSetAndGetChanged(MapType mapType,
			UpdateMode updateMode,
			Object2ObjectSortedMap<String, String> entries,
			boolean shouldFail) {
		var stpVer = run(shouldFail, () -> tempDb(getTempDbGenerator(), db -> {
			var map = tempDatabaseMapDictionaryMap(tempDictionary(db, updateMode), mapType, 5);
			var x1 = map.setAndGetChanged(entries);
			var x2 = map.setAndGetChanged(entries);
			if (!entries.isEmpty()) {
				map.remove(entries.keySet().stream().findFirst().orElseThrow());
			}
			var x3 = map.setAndGetChanged(entries);
			return Arrays.asList(x1, x2, x3);
		}));
		if (shouldFail) {
			this.checkLeaks = false;
		} else {
			Assertions.assertEquals(Arrays.asList(!entries.isEmpty(), false, !entries.isEmpty()), stpVer);
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsPutMulti")
	public void testSetAndGetPrevious(MapType mapType,
			UpdateMode updateMode,
			Object2ObjectSortedMap<String, String> entries,
			boolean shouldFail) {
		var stpVer = run(shouldFail, () -> tempDb(getTempDbGenerator(), db -> {
			var map = tempDatabaseMapDictionaryMap(tempDictionary(db, updateMode), mapType, 5);
			return Arrays.asList(map.setAndGetPrevious(entries), map.setAndGetPrevious(entries));
		}));
		if (shouldFail) {
			this.checkLeaks = false;
		} else {
			Assertions.assertEquals(Arrays.asList(null, entries.isEmpty() ? null : entries), stpVer);
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsPutMulti")
	public void testSetClearAndGetPreviousGet(MapType mapType,
			UpdateMode updateMode,
			Object2ObjectSortedMap<String, String> entries,
			boolean shouldFail) {
		var stpVer = run(shouldFail, () -> tempDb(getTempDbGenerator(), db -> {
			var map = tempDatabaseMapDictionaryMap(tempDictionary(db, updateMode), mapType, 5);
			map.set(entries);
			return Arrays.asList(map.clearAndGetPrevious(), map.get(null));
		}));
		if (shouldFail) {
			this.checkLeaks = false;
		} else {
			Assertions.assertEquals(Arrays.asList(entries.isEmpty() ? null : entries, null), stpVer);
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsPutMulti")
	public void testPutMultiGetAllValues(MapType mapType, UpdateMode updateMode, Object2ObjectSortedMap<String, String> entries, boolean shouldFail) {
		var stpVer = run(shouldFail, () -> tempDb(getTempDbGenerator(), db -> {
			var map = tempDatabaseMapDictionaryMap(tempDictionary(db, updateMode), mapType, 5);
			map.putMulti(entries.entrySet().stream());
			return toListClose(map.getAllValues(null, false));
		}));
		if (shouldFail) {
			this.checkLeaks = false;
		} else {
			Assertions.assertEquals(entries.entrySet(), Set.copyOf(stpVer));
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsPutMulti")
	public void testPutMultiGet(MapType mapType, UpdateMode updateMode, Object2ObjectSortedMap<String, String> entries, boolean shouldFail) {
		var stpVer = run(shouldFail, () -> tempDb(getTempDbGenerator(), db -> {
			var map = tempDatabaseMapDictionaryMap(tempDictionary(db, updateMode), mapType, 5);
			map.putMulti(entries.entrySet().stream());
			return map.get(null);
		}));
		if (shouldFail) {
			this.checkLeaks = false;
		} else {
			Assertions.assertEquals(entries.isEmpty() ? null : entries, stpVer);
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsPutMulti")
	public void testPutMultiGetAllStagesGet(MapType mapType, UpdateMode updateMode, Object2ObjectSortedMap<String, String> entries, boolean shouldFail) {
		var remainingEntries = new ArrayList<Entry<String, String>>();
		var stpVer = run(shouldFail, () -> tempDb(getTempDbGenerator(), db -> {
			var map = tempDatabaseMapDictionaryMap(tempDictionary(db, updateMode), mapType, 5);
			map.putMulti(entries.entrySet().stream());
			return toListClose(map.getAllStages(null, false).map(stage -> {
				var v = stage.getValue().get(null);
				if (v == null) {
					return null;
				}
				return Map.entry(stage.getKey(), v);
			}));
		}));
		if (shouldFail) {
			this.checkLeaks = false;
		} else {
			entries.forEach((k, v) -> remainingEntries.add(Map.entry(k, v)));
			Assertions.assertEquals(new HashSet<>(remainingEntries), Set.copyOf(stpVer));
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsPutMulti")
	public void testPutMultiIsEmpty(MapType mapType, UpdateMode updateMode, Object2ObjectSortedMap<String, String> entries, boolean shouldFail) {
		var stpVer = run(shouldFail, () -> tempDb(getTempDbGenerator(), db -> {
			var map = tempDatabaseMapDictionaryMap(tempDictionary(db, updateMode), mapType, 5);
			var x1 = map.isEmpty(null);
			map.putMulti(entries.entrySet().stream());
			var x2 = map.isEmpty(null);
			return Arrays.asList(x1, x2);
		}));
		if (shouldFail) {
			this.checkLeaks = false;
		} else {
			Assertions.assertEquals(Arrays.asList(true, entries.isEmpty()), stpVer);
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsPutMulti")
	public void testPutMultiClear(MapType mapType,
			UpdateMode updateMode,
			Object2ObjectSortedMap<String, String> entries,
			boolean shouldFail) {
		List<Boolean> result;
		try {
			result = run(shouldFail, () -> tempDb(getTempDbGenerator(), db -> {
				var map = tempDatabaseMapDictionaryMap(tempDictionary(db, updateMode), mapType, 5);
				var x1 = map.isEmpty(null);
				map.putMulti(entries.entrySet().stream());
				var x2 = map.isEmpty(null);
				map.clear();
				var x3 = map.isEmpty(null);
				return List.of(x1, x2, x3);
			}));
			Assertions.assertEquals(true, result.get(0));

			Assertions.assertEquals(entries.isEmpty(), result.get(1));

			Assertions.assertEquals(true, result.get(2));
		} catch (Exception ex) {
			if (shouldFail) {
				this.checkLeaks = false;
			} else {
				throw ex;
			}
		}
	}
}
