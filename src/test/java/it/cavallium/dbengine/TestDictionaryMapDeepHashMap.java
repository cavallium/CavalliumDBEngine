package it.cavallium.dbengine;

import static it.cavallium.dbengine.DbTestUtils.tempDatabaseMapDictionaryDeepMapHashMap;
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
import reactor.test.StepVerifier.Step;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuple3;
import reactor.util.function.Tuple4;
import reactor.util.function.Tuples;

public class TestDictionaryMapDeepHashMap {

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
		var goodKeys1 = Set.of("12345", "zebra");
		Set<String> badKeys1;
		if (isTestBadKeysEnabled()) {
			badKeys1 = Set.of("", "a", "aaaa", "aaaaaa");
		} else {
			badKeys1 = Set.of();
		}
		var goodKeys2 = Set.of("123456", "anatra", "", "a", "aaaaa", "aaaaaaa");

		var values = Set.of("a", "", "\0", "\0\0", "z", "azzszgzczqz", BIG_STRING);

		Flux<Tuple4<String, String, String, Boolean>> failOnKeys1 = Flux
				.fromIterable(badKeys1)
				.map(badKey1 -> Tuples.of(
						badKey1,
						goodKeys2.stream().findAny().orElseThrow(),
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
						failOnKeys1
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
						fullTuple.getT1() != UpdateMode.ALLOW || fullTuple.getT5()
				))
				.toStream();
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsPut")
	public void testAtPutValueGetAllValues(UpdateMode updateMode, String key1, String key2, String value, boolean shouldFail) {
		var stpVer = StepVerifier
				.create(tempDb(db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryDeepMapHashMap(dict, 5))
						.flatMapMany(map -> map
								.at(null, key1).flatMap(v -> v.putValue(key2, value).doFinally(s -> v.release()))
								.thenMany(map
										.getAllValues(null)
										.map(Entry::getValue)
										.flatMap(maps -> Flux.fromIterable(maps.entrySet()))
										.map(Entry::getValue)
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

}
