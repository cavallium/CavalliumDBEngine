package it.cavallium.dbengine;

import static it.cavallium.dbengine.DbTestUtils.destroyAllocator;
import static it.cavallium.dbengine.DbTestUtils.ensureNoLeaks;
import static it.cavallium.dbengine.DbTestUtils.newAllocator;
import static it.cavallium.dbengine.DbTestUtils.tempDb;

import it.cavallium.dbengine.DbTestUtils.TestAllocator;
import it.cavallium.dbengine.database.LLKeyValueDatabase;
import it.cavallium.dbengine.database.collections.DatabaseInt;
import it.cavallium.dbengine.database.collections.DatabaseLong;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public abstract class TestSingletons {

	private TestAllocator allocator;

	protected abstract TemporaryDbGenerator getTempDbGenerator();

	private static Stream<Arguments> provideNumberWithRepeats() {
		return Stream.of(
				Arguments.of(Integer.MIN_VALUE, 2),
				Arguments.of(-11, 2),
				Arguments.of(0, 3),
				Arguments.of(102, 5)
		);
	}

	private static Stream<Arguments> provideLongNumberWithRepeats() {
		return Stream.of(
				Arguments.of(Long.MIN_VALUE, 2),
				Arguments.of(-11L, 2),
				Arguments.of(0L, 3),
				Arguments.of(102L, 5)
		);
	}
	
	@BeforeEach
	public void beforeEach() {
		this.allocator = newAllocator();
		ensureNoLeaks(allocator.allocator(), false, false);
	}

	@AfterEach
	public void afterEach() {
		ensureNoLeaks(allocator.allocator(), true, false);
		destroyAllocator(allocator);
	}

	@Test
	public void testCreateInteger() {
		StepVerifier
				.create(tempDb(getTempDbGenerator(), allocator, db -> tempInt(db, "test", 0)
						.flatMap(dbInt -> dbInt.get(null))
						.then()
				))
				.verifyComplete();
	}

	@Test
	public void testCreateIntegerNoop() {
		StepVerifier
				.create(tempDb(getTempDbGenerator(), allocator, db -> tempInt(db, "test", 0)
						.then()
				))
				.verifyComplete();
	}

	@Test
	public void testCreateLong() {
		StepVerifier
				.create(tempDb(getTempDbGenerator(), allocator, db -> tempLong(db, "test", 0)
						.flatMap(dbLong -> dbLong.get(null))
						.then()
				))
				.verifyComplete();
	}

	@ParameterizedTest
	@ValueSource(ints = {Integer.MIN_VALUE, -192, -2, -1, 0, 1, 2, 1292, Integer.MAX_VALUE})
	public void testDefaultValueInteger(int i) {
		StepVerifier
				.create(tempDb(getTempDbGenerator(), allocator, db -> tempInt(db, "test", i)
						.flatMap(dbInt -> dbInt.get(null))
				))
				.expectNext(i)
				.verifyComplete();
	}

	@ParameterizedTest
	@ValueSource(longs = {Long.MIN_VALUE, -192, -2, -1, 0, 1, 2, 1292, Long.MAX_VALUE})
	public void testDefaultValueLong(long i) {
		StepVerifier
				.create(tempDb(getTempDbGenerator(), allocator, db -> tempLong(db, "test", i)
						.flatMap(dbLong -> dbLong.get(null))
				))
				.expectNext(i)
				.verifyComplete();
	}

	@ParameterizedTest
	@MethodSource("provideNumberWithRepeats")
	public void testSetInteger(Integer i, Integer repeats) {
		StepVerifier
				.create(tempDb(getTempDbGenerator(), allocator, db -> tempInt(db, "test", 0)
						.flatMap(dbInt -> Mono
								.defer(() -> dbInt.set((int) System.currentTimeMillis()))
								.repeat(repeats)
								.then(dbInt.set(i))
								.then(dbInt.get(null)))
				))
				.expectNext(i)
				.verifyComplete();
	}

	@ParameterizedTest
	@MethodSource("provideLongNumberWithRepeats")
	public void testSetLong(Long i, Integer repeats) {
		StepVerifier
				.create(tempDb(getTempDbGenerator(), allocator, db -> tempLong(db, "test", 0)
						.flatMap(dbLong -> Mono
								.defer(() -> dbLong.set(System.currentTimeMillis()))
								.repeat(repeats)
								.then(dbLong.set(i))
								.then(dbLong.get(null)))
				))
				.expectNext(i)
				.verifyComplete();
	}

	public static Mono<DatabaseInt> tempInt(LLKeyValueDatabase database, String name, int defaultValue) {
		return database
				.getInteger("ints", name, defaultValue);
	}

	public static Mono<DatabaseLong> tempLong(LLKeyValueDatabase database, String name, long defaultValue) {
		return database
				.getLong("longs", name, defaultValue);
	}
}
