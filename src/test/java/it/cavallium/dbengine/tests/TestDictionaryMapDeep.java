package it.cavallium.dbengine.tests;

import static it.cavallium.dbengine.tests.DbTestUtils.BIG_STRING;
import static it.cavallium.dbengine.tests.DbTestUtils.ensureNoLeaks;
import static it.cavallium.dbengine.tests.DbTestUtils.isCIMode;
import static it.cavallium.dbengine.tests.DbTestUtils.run;
import static it.cavallium.dbengine.tests.DbTestUtils.runVoid;
import static it.cavallium.dbengine.tests.DbTestUtils.tempDatabaseMapDictionaryDeepMap;
import static it.cavallium.dbengine.tests.DbTestUtils.tempDb;
import static it.cavallium.dbengine.tests.DbTestUtils.tempDictionary;

import com.google.common.collect.Streams;
import it.cavallium.dbengine.database.UpdateMode;
import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectSortedMap;
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

	@ParameterizedTest
	@MethodSource("provideArgumentsSet")
	public void testSetValueGetAllValues(UpdateMode updateMode, String key, Object2ObjectSortedMap<String, String> value,
			boolean shouldFail) {
		var result = run(shouldFail, () -> tempDb(getTempDbGenerator(), db -> {
			var map = tempDatabaseMapDictionaryDeepMap(tempDictionary(db, updateMode), 5, 6);
			map.putValue(key, value);
			try (var stream = map.getAllValues(null, false)) {
				return stream.toList();
			}
		}));
		if (shouldFail) {
			this.checkLeaks = false;
		} else {
			Assertions.assertEquals(List.of(Map.entry(key, value)), result);
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsSet")
	public void testAtSetGetAllStagesGetAllValues(UpdateMode updateMode,
			String key,
			Object2ObjectSortedMap<String, String> value,
			boolean shouldFail) {
		var remainingEntries = new ConcurrentHashMap<Tuple3<String, String, String>, Boolean>().keySet(true);
		List<Tuple3<String, String, String>> result = run(shouldFail, () -> tempDb(getTempDbGenerator(), db -> {
			var dict = tempDictionary(db, updateMode);
			var map = tempDatabaseMapDictionaryDeepMap(dict, 5, 6);
			var v = map.at(null, key);
			v.set(value);
			var v2 = map.at(null, "capra");
			v2.set(new Object2ObjectLinkedOpenHashMap<>(Map.of("normal", "123", "ormaln", "456")));
			try (var stages = map.getAllStages(null, false)) {
				return stages
						.flatMap(stage -> stage
								.getValue()
								.getAllValues(null, false)
								.map(r -> new Tuple3<>(stage.getKey(), r.getKey(), r.getValue())))
						.toList();
			}
		}));
		if (shouldFail) {
			this.checkLeaks = false;
		} else {
			value.forEach((k, v) -> remainingEntries.add(new Tuple3<>(key, k, v)));
			remainingEntries.add(new Tuple3<>("capra", "normal", "123"));
			remainingEntries.add(new Tuple3<>("capra", "ormaln", "456"));
			Assertions.assertEquals(remainingEntries, new HashSet<>(result));
		}
	}

	@ParameterizedTest
	@MethodSource({"provideArgumentsPut"})
	public void testAtPutValueAtGetValue(UpdateMode updateMode, String key1, String key2, String value,
			boolean shouldFail) {
		var result = run(shouldFail, () -> tempDb(getTempDbGenerator(), db -> {
			var map = tempDatabaseMapDictionaryDeepMap(tempDictionary(db, updateMode), 5, 6);
			var v = map.at(null, key1);
			v.putValue(key2, value);
			var v2 = map.at(null, key1);
			return v2.getValue(null, key2);
		}));
		if (shouldFail) {
			this.checkLeaks = false;
		} else {
			Assertions.assertEquals(result, value);
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsSet")
	public void testSetAndGetPrevious(UpdateMode updateMode,
			String key,
			Object2ObjectSortedMap<String, String> value,
			boolean shouldFail) {
		var result = run(shouldFail, () -> tempDb(getTempDbGenerator(), db -> {
					var dict = tempDictionary(db, updateMode);
					var map = tempDatabaseMapDictionaryDeepMap(dict, 5, 6);
					var x1 = Objects.requireNonNullElse(
							map.putValueAndGetPrevious(key, new Object2ObjectLinkedOpenHashMap<>(Map.of("error?", "error."))),
							new Object2ObjectLinkedOpenHashMap<>(Map.of("nothing", "nothing"))
					);
					var x2 = Objects.requireNonNull(map.putValueAndGetPrevious(key, value));
					var x3 = Objects.requireNonNull(map.putValueAndGetPrevious(key, value));
					return List.of(x1, x2, x3);
				}));
		if (shouldFail) {
			this.checkLeaks = false;
		} else {
			Assertions.assertEquals(List.of(new Object2ObjectLinkedOpenHashMap<>(Map.of("nothing", "nothing")),
					new Object2ObjectLinkedOpenHashMap<>(Map.of("error?", "error.")), value), result);
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsPut")
	public void testAtPutValueAndGetPrevious(UpdateMode updateMode, String key1, String key2, String value,
			boolean shouldFail) throws IOException {
		var stpVer = run(shouldFail, () -> tempDb(getTempDbGenerator(), db -> {
			var map = tempDatabaseMapDictionaryDeepMap(tempDictionary(db, updateMode), 5, 6);
			var x1 = map.at(null, key1).putValueAndGetPrevious(key2, "error?");
			var x2 = map.at(null, key1).putValueAndGetPrevious(key2, value);
			var x3 = map.at(null, key1).putValueAndGetPrevious(key2, value);
			return Stream.of(x1, x2, x3).filter(Objects::nonNull).toList();
		}));
		if (shouldFail) {
			this.checkLeaks = false;
		} else {
			Assertions.assertEquals(List.of("error?", value), stpVer);
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsSet")
	public void testSetValueRemoveAndGetPrevious(UpdateMode updateMode,
			String key,
			Object2ObjectSortedMap<String, String> value,
			boolean shouldFail) {
		var stpVer = run(shouldFail, () -> tempDb(getTempDbGenerator(), db -> {
					var map = tempDatabaseMapDictionaryDeepMap(tempDictionary(db, updateMode), 5, 6);
					var x1 = map.removeAndGetPrevious(key);
					map.putValue(key, value);
					var x2 = map.removeAndGetPrevious(key);
					var x3 = map.removeAndGetPrevious(key);
					return Stream.of(x1, x2, x3).filter(Objects::nonNull).toList();
				}));
		if (shouldFail) {
			this.checkLeaks = false;
		} else {
			Assertions.assertEquals(List.of(value), stpVer);
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsPut")
	public void testAtPutValueRemoveAndGetPrevious(UpdateMode updateMode, String key1, String key2, String value,
			boolean shouldFail) {
		var stpVer = run(shouldFail, () -> tempDb(getTempDbGenerator(), db -> {
			var map = tempDatabaseMapDictionaryDeepMap(tempDictionary(db, updateMode), 5, 6);
			var stage1 = map.at(null, key1);
			stage1.putValue(key2, "error?");
			var x1 = stage1.removeAndGetPrevious(key2);
			var stage2 = map.at(null, key1);
			stage2.putValue(key2, value);
			var x2 = stage2.removeAndGetPrevious(key2);
			var stage3 = map.at(null, key1);
			var x3 = stage3.removeAndGetPrevious(key2);
			return Stream.of(x1, x2, x3).filter(Objects::nonNull).toList();
		}));
		if (shouldFail) {
			this.checkLeaks = false;
		} else {
			Assertions.assertEquals(List.of("error?", value), stpVer);
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsSet")
	public void testSetValueRemoveAndGetStatus(UpdateMode updateMode,
			String key,
			Object2ObjectSortedMap<String, String> value,
			boolean shouldFail) {
		var stpVer = run(shouldFail, () -> tempDb(getTempDbGenerator(), db -> {
			var map = tempDatabaseMapDictionaryDeepMap(tempDictionary(db, updateMode), 5, 6);
			var x1 = map.removeAndGetStatus(key);
			map.putValue(key, value);
			var x2 = map.removeAndGetStatus(key);
			var x3 = map.removeAndGetStatus(key);
			return Stream.of(x1, x2, x3).toList();
		}));
		if (shouldFail) {
			this.checkLeaks = false;
		} else {
			Assertions.assertEquals(List.of(false, true, false), stpVer);
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsPut")
	public void testAtPutValueRemoveAndGetStatus(UpdateMode updateMode, String key1, String key2, String value,
			boolean shouldFail) {
		var stpVer = run(shouldFail, () -> tempDb(getTempDbGenerator(), db -> {
					var map = tempDatabaseMapDictionaryDeepMap(tempDictionary(db, updateMode), 5, 6);
					var step1 = map.at(null, key1);
					step1.putValue(key2, "error?");
					var x1 = step1.removeAndGetStatus(key2);
					var step2 = map.at(null, key1);
					step2.putValue(key2, value);
					var x2 = step2.removeAndGetStatus(key2);
					var step3 = map.at(null, key1);
					var x3 = step3.removeAndGetStatus(key2);
					return Stream.of(x1, x2, x3).toList();
				}));
		if (shouldFail) {
			this.checkLeaks = false;
		} else {
			Assertions.assertEquals(List.of(true, true, false), stpVer);
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
		var stpVer = run(shouldFail || updateMode != UpdateMode.ALLOW_UNSAFE, () -> tempDb(getTempDbGenerator(), db -> {
			var map = tempDatabaseMapDictionaryDeepMap(tempDictionary(db, updateMode), 5, 6);
			return Stream.of(map.updateValue(key, old -> {
				assert old == null;
				return new Object2ObjectLinkedOpenHashMap<>(Map.of("error?", "error."));
			}), map.updateValue(key, old -> {
				assert Objects.equals(old, Map.of("error?", "error."));
				return new Object2ObjectLinkedOpenHashMap<>(Map.of("error?", "error."));
			}), map.updateValue(key, old -> {
				assert Objects.equals(old, Map.of("error?", "error."));
				return new Object2ObjectLinkedOpenHashMap<>(Map.of("error?", "error."));
			}), map.updateValue(key, old -> {
				assert Objects.equals(old, Map.of("error?", "error."));
				return value;
			}), map.updateValue(key, old -> {
				assert Objects.equals(old, value);
				return value;
			})).toList();
		}));
		if (updateMode == UpdateMode.ALLOW_UNSAFE && !shouldFail) {
			Assertions.assertEquals(List.of(true, false, false, true, false), stpVer);
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsPut")
	public void testAtUpdate(UpdateMode updateMode, String key1, String key2, String value, boolean shouldFail) {
		if (updateMode == UpdateMode.DISALLOW && !isTestBadKeysEnabled()) {
			return;
		}
		var stpVer = run(shouldFail || updateMode == UpdateMode.DISALLOW, () -> tempDb(getTempDbGenerator(), db -> {
			var map = tempDatabaseMapDictionaryDeepMap(tempDictionary(db, updateMode), 5, 6);
					var x1 = map.at(null, key1).updateValue(key2, prev -> prev);
					var x2 = map.at(null, key1).updateValue(key2, prev -> value);
					var x3 = map.at(null, key1).updateValue(key2, prev -> value);
					var x4 = map.at(null, key1).updateValue(key2, prev -> null);
					return Stream.of(x1, x2, x3, x4).toList();
				}));
		if (updateMode != UpdateMode.DISALLOW && !shouldFail) {
			Assertions.assertEquals(List.of(false, true, false, true), stpVer);
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
		var stpVer = run(shouldFail || updateMode != UpdateMode.ALLOW_UNSAFE, () -> tempDb(getTempDbGenerator(), db -> {
			var map = tempDatabaseMapDictionaryDeepMap(tempDictionary(db, updateMode), 5, 6);
			map.updateValue(key, old -> {
				assert old == null;
				return new Object2ObjectLinkedOpenHashMap<>(Map.of("error?", "error."));
			});
			var x1 = map.getValue(null, key);
			map.updateValue(key, old -> {
				assert Objects.equals(old, Map.of("error?", "error."));
				return new Object2ObjectLinkedOpenHashMap<>(Map.of("error?", "error."));
			});
			var x2 = map.getValue(null, key);

			map.updateValue(key, old -> {
				assert Objects.equals(old, Map.of("error?", "error."));
				return new Object2ObjectLinkedOpenHashMap<>(Map.of("error?", "error."));
			});
			var x3 = map.getValue(null, key);
			map.updateValue(key, old -> {
				assert Objects.equals(old, new Object2ObjectLinkedOpenHashMap<>(Map.of("error?", "error.")));
				return value;
			});
			var x4 = map.getValue(null, key);
			map.updateValue(key, old -> {
				assert Objects.equals(old, value);
				return value;
			});
			var x5 = map.getValue(null, key);
			return Stream.of(x1, x2, x3, x4, x5).toList();
		}));
		if (updateMode == UpdateMode.ALLOW_UNSAFE && !shouldFail) {
			Assertions.assertEquals(List.of(new Object2ObjectLinkedOpenHashMap<>(Map.of("error?", "error.")),
					new Object2ObjectLinkedOpenHashMap<>(Map.of("error?", "error.")),
					new Object2ObjectLinkedOpenHashMap<>(Map.of("error?", "error.")),
					value,
					value
			), stpVer);
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsPut")
	public void testAtUpdateGetValue(UpdateMode updateMode, String key1, String key2, String value, boolean shouldFail) {
		if (updateMode == UpdateMode.DISALLOW && !isTestBadKeysEnabled()) {
			return;
		}
		var stpVer = run(shouldFail || updateMode == UpdateMode.DISALLOW, () -> tempDb(getTempDbGenerator(), db -> {
			var map = tempDatabaseMapDictionaryDeepMap(tempDictionary(db, updateMode), 5, 6);
			var stage1 = map.at(null, key1);
			stage1.updateValue(key2, prev -> prev);
			var x1 = Objects.requireNonNullElse(stage1.getValue(null, key2), "empty");
			var stage2 = map.at(null, key1);
			stage2.updateValue(key2, prev -> value);
			var x2 = Objects.requireNonNullElse(stage2.getValue(null, key2), "empty");
			var stage3 = map.at(null, key1);
			stage3.updateValue(key2, prev -> value);
			var x3 = Objects.requireNonNullElse(stage3.getValue(null, key2), "empty");
			var stage4 = map.at(null, key1);
			stage4.updateValue(key2, prev -> null);
			var x4 = Objects.requireNonNullElse(stage4.getValue(null, key2), "empty");
			return Stream.of(x1, x2, x3, x4).toList();
		}));
		if (updateMode != UpdateMode.DISALLOW && !shouldFail) {
			Assertions.assertEquals(List.of("empty", value, value, "empty"), stpVer);
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsSet")
	public void testSetAndGetChanged(UpdateMode updateMode,
			String key,
			Object2ObjectSortedMap<String, String> value,
			boolean shouldFail) {
		var stpVer = run(shouldFail, () -> tempDb(getTempDbGenerator(), db -> {
			var map = tempDatabaseMapDictionaryDeepMap(tempDictionary(db, updateMode), 5, 6);
			var x1 = map.putValueAndGetChanged(key, new Object2ObjectLinkedOpenHashMap<>(Map.of("error?", "error.")));
			var x2 = map.putValueAndGetChanged(key, value);
			var x3 = map.putValueAndGetChanged(key, value);
			map.remove(key);
			var x4 = map.putValueAndGetChanged(key, new Object2ObjectLinkedOpenHashMap<>(Map.of("error?", "error.")));
			return List.of(x1, x2, x3, x4);
		}));
		if (shouldFail) {
			this.checkLeaks = false;
		} else {
			Assertions.assertEquals(List.of(true, true, false, true), stpVer);
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
				.map(keyTuple -> new Tuple2<>(Streams
						.zip(keyTuple.getT1().stream(), values.stream(), Tuple2::new)
						.collect(Collectors.toMap(Tuple2::getT1, Tuple2::getT2, (a, b) -> a, Object2ObjectLinkedOpenHashMap::new)),
						keyTuple.getT2()
				))
				.flatMap(entryTuple -> Arrays
						.stream(UpdateMode.values())
						.map(updateMode -> new Tuple3<>(updateMode, entryTuple.getT1(), entryTuple.getT2())))
				.map(fullTuple -> Arguments.of(fullTuple.getT1(), fullTuple.getT2(), fullTuple.getT3()));
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsSetMulti")
	public void testSetMultiGetMulti(UpdateMode updateMode,
			Map<String, Object2ObjectSortedMap<String, String>> entries,
			boolean shouldFail) {
		var flux = run(shouldFail, () -> tempDb(getTempDbGenerator(), db -> {
			var map = tempDatabaseMapDictionaryDeepMap(tempDictionary(db, updateMode), 5, 6);
			var entriesFlux = entries.entrySet();
			var keysFlux = entriesFlux.stream().map(Entry::getKey).toList();
			map.putMulti(entriesFlux.stream());
			var resultsFlux = map.getMulti(null, keysFlux.stream());
			return Streams
					.zip(keysFlux.stream(), resultsFlux, Map::entry)
					.filter(k -> k.getValue().isPresent())
					.map(k -> Map.entry(k.getKey(), k.getValue().orElseThrow()))
					.toList();
		}));
		if (shouldFail) {
			this.checkLeaks = false;
		} else {
			Assertions.assertEquals(entries.entrySet().stream().toList(), flux);
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsSetMulti")
	public void testSetAllValuesGetMulti(UpdateMode updateMode,
			Object2ObjectSortedMap<String, Object2ObjectSortedMap<String, String>> entries,
			boolean shouldFail) {
		var remainingEntries = new ArrayList<Entry<String, Map<String, String>>>();
		var stpVer = run(shouldFail, () -> tempDb(getTempDbGenerator(), db -> {
			var map = tempDatabaseMapDictionaryDeepMap(tempDictionary(db, updateMode), 5, 6);
			var entriesFlux = entries.entrySet();
			var keysFlux = entriesFlux.stream().map(Entry::getKey).toList();
			map.setAllValues(entries.entrySet().stream());
			try (var resultsFlux = map.getMulti(null, entries.keySet().stream())) {
				return Streams
						.zip(keysFlux.stream(), resultsFlux, Map::entry)
						.filter(k -> k.getValue().isPresent())
						.map(k -> Map.entry(k.getKey(), k.getValue().orElseThrow()))
						.toList();
			}
		}));
		if (shouldFail) {
			this.checkLeaks = false;
		} else {
			entries.forEach((k, v) -> remainingEntries.add(Map.entry(k, v)));
			Assertions.assertEquals(remainingEntries, stpVer);
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsSetMulti")
	public void testSetAllValuesAndGetPrevious(UpdateMode updateMode,
			Object2ObjectSortedMap<String, Object2ObjectSortedMap<String, String>> entries,
			boolean shouldFail) {
		var remainingEntries = new ArrayList<Entry<String, Object2ObjectSortedMap<String, String>>>();
		var stpVer = run(shouldFail, () -> tempDb(getTempDbGenerator(), db -> {
			var map = tempDatabaseMapDictionaryDeepMap(tempDictionary(db, updateMode), 5, 6);
			List<Entry<String, Object2ObjectSortedMap<String, String>>> a1;
			try (var stream1 = map.setAllValuesAndGetPrevious(entries.entrySet().stream())) {
				a1 = stream1.toList();
			}
			List<Entry<String, Object2ObjectSortedMap<String, String>>> a2;
			try (var stream2 = map.setAllValuesAndGetPrevious(entries.entrySet().stream())) {
				a2 = stream2.toList();
			}
			return List.of(a1, a2);
		}));
		if (shouldFail) {
			this.checkLeaks = false;
		} else {
			entries.forEach((k, v) -> remainingEntries.add(Map.entry(k, v)));
			Assertions.assertEquals(
					List.of(List.of(), remainingEntries),
					stpVer
			);
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsSetMulti")
	public void testSetGetMulti(UpdateMode updateMode, Object2ObjectSortedMap<String, Object2ObjectSortedMap<String, String>> entries, boolean shouldFail) {
		var remainingEntries = new ArrayList<Entry<String, Object2ObjectSortedMap<String, String>>>();
		var stpVer = run(shouldFail, () -> tempDb(getTempDbGenerator(), db -> {
			var map = tempDatabaseMapDictionaryDeepMap(tempDictionary(db, updateMode), 5, 6);
			var entriesFlux = entries.entrySet();
			var keysFlux = entriesFlux.stream().map(Entry::getKey).toList();
			map.set(entries);
			List<Optional<Object2ObjectSortedMap<String, String>>> results;
			try (var stream = map.getMulti(null, entries.keySet().stream())) {
				results = stream.toList();
			}
			return Streams
					.zip(keysFlux.stream(), results.stream(), Map::entry)
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
	@MethodSource("provideArgumentsSetMulti")
	public void testSetAndGetStatus(UpdateMode updateMode,
			Object2ObjectSortedMap<String, Object2ObjectSortedMap<String, String>> entries,
			boolean shouldFail) {
		var stpVer = run(shouldFail, () -> tempDb(getTempDbGenerator(), db -> {
			var map = tempDatabaseMapDictionaryDeepMap(tempDictionary(db, updateMode), 5, 6);
			var x1 = map.setAndGetChanged(entries);
			var x2 = map.setAndGetChanged(entries);
			if (!entries.isEmpty()) {
				map.remove(entries.keySet().stream().findAny().orElseThrow());
			}
			var x3 = map.setAndGetChanged(entries);
			return List.of(x1, x2, x3);
		}));
		if (shouldFail) {
			this.checkLeaks = false;
		} else {
			Assertions.assertEquals(List.of(!entries.isEmpty(), false, !entries.isEmpty()), stpVer);
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsSetMulti")
	public void testSetAndGetPrevious(UpdateMode updateMode, Object2ObjectSortedMap<String, Object2ObjectSortedMap<String, String>> entries,
			boolean shouldFail) {
		var remainingEntries = new ArrayList<Entry<String, Object2ObjectSortedMap<String, String>>>();
		var stpVer = run(shouldFail, () -> tempDb(getTempDbGenerator(), db -> {
			var map = tempDatabaseMapDictionaryDeepMap(tempDictionary(db, updateMode), 5, 6);
			var x1 = map.setAndGetPrevious(entries);
			var x2 = map.setAndGetPrevious(entries);
			return Stream.of(x1, x2).map(x -> x != null ? x.entrySet().stream().toList() : null).toList();
		}));
		if (shouldFail) {
			this.checkLeaks = false;
		} else {
			entries.forEach((k, v) -> remainingEntries.add(Map.entry(k, v)));
			Assertions.assertEquals(Arrays.asList(null, remainingEntries.isEmpty() ? null : remainingEntries), stpVer);
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsSetMulti")
	public void testSetClearAndGetPreviousGet(UpdateMode updateMode, Object2ObjectSortedMap<String, Object2ObjectSortedMap<String, String>> entries,
			boolean shouldFail) {
		var remainingEntries = new ArrayList<Entry<String, Object2ObjectSortedMap<String, String>>>();
		var stpVer = run(shouldFail, () -> tempDb(getTempDbGenerator(), db -> {
					var map = tempDatabaseMapDictionaryDeepMap(tempDictionary(db, updateMode), 5, 6);
					map.set(entries);
			var prev = map.clearAndGetPrevious();
			var curr = map.get(null);
			return Stream.of(prev, curr).map(x -> x != null ? x.entrySet().stream().toList() : null).toList();
				}));
		if (shouldFail) {
			this.checkLeaks = false;
		} else {
			entries.forEach((k, v) -> remainingEntries.add(Map.entry(k, v)));
			Assertions.assertEquals(Arrays.asList(remainingEntries.isEmpty() ? null : remainingEntries, null), stpVer);
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsSetMulti")
	public void testSetMultiGetAllValues(UpdateMode updateMode,
			Object2ObjectSortedMap<String, Object2ObjectSortedMap<String, String>> entries,
			boolean shouldFail) {
		var remainingEntries = new ArrayList<Entry<String, Object2ObjectSortedMap<String, String>>>();
		var stpVer = run(shouldFail, () -> tempDb(getTempDbGenerator(), db -> {
			var map = tempDatabaseMapDictionaryDeepMap(tempDictionary(db, updateMode), 5, 6);
			map.putMulti(entries.entrySet().stream());
			try (var values = map.getAllValues(null, false)) {
				return values.toList();
			}
		}));
		if (shouldFail) {
			this.checkLeaks = false;
		} else {
			entries.forEach((k, v) -> remainingEntries.add(Map.entry(k, v)));
			Assertions.assertEquals(remainingEntries, stpVer);
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsSetMulti")
	public void testSetMultiGet(UpdateMode updateMode, Object2ObjectSortedMap<String, Object2ObjectSortedMap<String, String>> entries, boolean shouldFail) {
		var remainingEntries = new ArrayList<Entry<String, Object2ObjectSortedMap<String, String>>>();
		var stpVer = run(shouldFail, () -> tempDb(getTempDbGenerator(), db -> {
			var map = tempDatabaseMapDictionaryDeepMap(tempDictionary(db, updateMode), 5, 6);
			map.putMulti(entries.entrySet().stream());
			return map.get(null);
		}));
		if (shouldFail) {
			this.checkLeaks = false;
		} else {
			entries.forEach((k, v) -> remainingEntries.add(Map.entry(k, v)));
			Assertions.assertEquals(remainingEntries.isEmpty() ? null : remainingEntries, stpVer == null ? null : stpVer.entrySet().stream().toList());
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsSetMulti")
	public void testSetMultiGetAllStagesGet(UpdateMode updateMode, Object2ObjectSortedMap<String, Object2ObjectSortedMap<String, String>> entries,
			boolean shouldFail) {
		var remainingEntries = new ArrayList<Entry<String, Object2ObjectSortedMap<String, String>>>();
		var stpVer = run(shouldFail, () -> tempDb(getTempDbGenerator(), db -> {
					var map = tempDatabaseMapDictionaryDeepMap(tempDictionary(db, updateMode), 5, 6);
					map.putMulti(entries.entrySet().stream());
					return map.getAllStages(null, false).map(stage -> {
						var v = stage.getValue().get(null);
						if (v == null) return null;
						return Map.entry(stage.getKey(), v);
					}).filter(Objects::nonNull).toList();
				}));
		if (shouldFail) {
			this.checkLeaks = false;
		} else {
			entries.forEach((k, v) -> remainingEntries.add(Map.entry(k, v)));
			Assertions.assertEquals(remainingEntries, stpVer);
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsSetMulti")
	public void testSetMultiIsEmpty(UpdateMode updateMode, Object2ObjectSortedMap<String, Object2ObjectSortedMap<String, String>> entries,
			boolean shouldFail) {
		var stpVer = run(shouldFail, () -> tempDb(getTempDbGenerator(), db -> {
			var map = tempDatabaseMapDictionaryDeepMap(tempDictionary(db, updateMode), 5, 6);
			var x1 = map.isEmpty(null);
			map.putMulti(entries.entrySet().stream());
			var x2 = map.isEmpty(null);
			return List.of(x1, x2);
		}));
		if (shouldFail) {
			this.checkLeaks = false;
		} else {
			Assertions.assertEquals(List.of(true, entries.isEmpty()), stpVer);
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsSetMulti")
	public void testSetMultiClear(UpdateMode updateMode, Object2ObjectSortedMap<String, Object2ObjectSortedMap<String, String>> entries, boolean shouldFail) {
		var stpVer = run(shouldFail, () -> tempDb(getTempDbGenerator(), db -> {
			var map = tempDatabaseMapDictionaryDeepMap(tempDictionary(db, updateMode), 5, 6);

			var x1 = map.isEmpty(null);
			map.putMulti(entries.entrySet().stream());
			var x2 = map.isEmpty(null);
			map.clear();
			var x3 = map.isEmpty(null);
			return List.of(x1, x2, x3);
		}));
		if (shouldFail) {
			this.checkLeaks = false;
		} else {
			Assertions.assertEquals(List.of(true, entries.isEmpty(), true), stpVer);
		}
	}
}
