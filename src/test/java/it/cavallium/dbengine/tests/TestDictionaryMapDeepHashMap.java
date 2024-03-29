package it.cavallium.dbengine.tests;

import static it.cavallium.dbengine.tests.DbTestUtils.BIG_STRING;
import static it.cavallium.dbengine.tests.DbTestUtils.ensureNoLeaks;
import static it.cavallium.dbengine.tests.DbTestUtils.isCIMode;
import static it.cavallium.dbengine.tests.DbTestUtils.run;
import static it.cavallium.dbengine.tests.DbTestUtils.tempDatabaseMapDictionaryDeepMapHashMap;
import static it.cavallium.dbengine.tests.DbTestUtils.tempDb;
import static it.cavallium.dbengine.tests.DbTestUtils.tempDictionary;
import static it.cavallium.dbengine.utils.StreamUtils.toList;

import com.google.common.collect.Streams;
import it.cavallium.dbengine.database.UpdateMode;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public abstract class TestDictionaryMapDeepHashMap {
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
		var goodKeys1 = isCIMode() ? List.of("12345") : List.of("12345", "zebra");
		List<String> badKeys1;
		if (isTestBadKeysEnabled()) {
			badKeys1 = List.of("", "a", "aaaa", "aaaaaa");
		} else {
			badKeys1 = List.of();
		}
		var goodKeys2 = isCIMode() ? List.of("123456") : List.of("123456", "anatra", "", "a", "aaaaa", "aaaaaaa");

		var values = isCIMode() ? List.of("val") : List.of("a", "", "\0", "\0\0", "z", "azzszgzczqz", BIG_STRING);

		Stream<Tuple4<String, String, String, Boolean>> failOnKeys1 = badKeys1.stream()
				.map(badKey1 -> new Tuple4<>(
						badKey1,
						goodKeys2.stream().findAny().orElseThrow(),
						values.stream().findAny().orElseThrow(),
						true
				));

		Stream<Tuple4<String, String, String, Boolean>> goodKeys1And2 = values.stream()
				.map(value -> new Tuple4<>(
						goodKeys1.stream().findAny().orElseThrow(),
						goodKeys2.stream().findAny().orElseThrow(),
						value,
						false
				));

		Stream<Tuple4<String, String, String, Boolean>> keys1And2 = Streams
				.concat(
						goodKeys1And2,
						failOnKeys1
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
						fullTuple.getT1() != UpdateMode.ALLOW || fullTuple.getT5()
				));
	}

	@BeforeEach
	public void beforeEach() {
		ensureNoLeaks();
	}

	@AfterEach
	public void afterEach() {
		if (!isCIMode() && checkLeaks) {
			ensureNoLeaks();
		}
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsPut")
	public void testAtPutValueGetAllValues(UpdateMode updateMode, String key1, String key2, String value, boolean shouldFail) {
		var stpVer = run(shouldFail, () -> tempDb(getTempDbGenerator(), db -> {
					var map = tempDatabaseMapDictionaryDeepMapHashMap(tempDictionary(db, updateMode), 5);
					map.at(null, key1).putValue(key2, value);
					return toList(map
							.getAllEntries(null, false)
							.map(Entry::getValue)
							.flatMap(maps -> maps.entrySet().stream())
							.map(Entry::getValue));
				}));
		if (shouldFail) {
			this.checkLeaks = false;
		} else {
			Assertions.assertEquals(List.of(value), stpVer);
		}
	}

}
