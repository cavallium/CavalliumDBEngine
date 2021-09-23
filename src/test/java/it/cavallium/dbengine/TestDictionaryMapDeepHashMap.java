package it.cavallium.dbengine;

import static it.cavallium.dbengine.DbTestUtils.BIG_STRING;
import static it.cavallium.dbengine.DbTestUtils.destroyAllocator;
import static it.cavallium.dbengine.DbTestUtils.ensureNoLeaks;
import static it.cavallium.dbengine.DbTestUtils.isCIMode;
import static it.cavallium.dbengine.DbTestUtils.newAllocator;
import static it.cavallium.dbengine.DbTestUtils.tempDatabaseMapDictionaryDeepMapHashMap;
import static it.cavallium.dbengine.DbTestUtils.tempDb;
import static it.cavallium.dbengine.DbTestUtils.tempDictionary;

import it.cavallium.dbengine.DbTestUtils.TestAllocator;
import it.cavallium.dbengine.database.UpdateMode;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
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
import reactor.util.function.Tuple3;
import reactor.util.function.Tuple4;
import reactor.util.function.Tuples;

public abstract class TestDictionaryMapDeepHashMap {

	private TestAllocator allocator;
	private boolean checkLeaks = true;

	private static boolean isTestBadKeysEnabled() {
		return !isCIMode() && System.getProperty("badkeys", "true").equalsIgnoreCase("true");
	}

	protected abstract TemporaryDbGenerator getTempDbGenerator();

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

	@BeforeEach
	public void beforeEach() {
		this.allocator = newAllocator();
		ensureNoLeaks(allocator.allocator(), false, false);
	}

	@AfterEach
	public void afterEach() {
		if (!isCIMode() && checkLeaks) {
			ensureNoLeaks(allocator.allocator(), true, false);
		}
		destroyAllocator(allocator);
	}

	@ParameterizedTest
	@MethodSource("provideArgumentsPut")
	public void testAtPutValueGetAllValues(UpdateMode updateMode, String key1, String key2, String value, boolean shouldFail) {
		var stpVer = StepVerifier
				.create(tempDb(getTempDbGenerator(), allocator, db -> tempDictionary(db, updateMode)
						.map(dict -> tempDatabaseMapDictionaryDeepMapHashMap(dict, 5))
						.flatMapMany(map -> map
								.at(null, key1).flatMap(v -> v.putValue(key2, value).doFinally(s -> v.close()))
								.thenMany(map
										.getAllValues(null)
										.map(Entry::getValue)
										.flatMap(maps -> Flux.fromIterable(maps.entrySet()))
										.map(Entry::getValue)
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

}
