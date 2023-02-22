package it.cavallium.dbengine.tests;

import static it.cavallium.dbengine.tests.DbTestUtils.ensureNoLeaks;
import static it.cavallium.dbengine.tests.DbTestUtils.tempDb;

import it.cavallium.dbengine.database.LLKeyValueDatabase;
import it.cavallium.dbengine.database.collections.DatabaseInt;
import it.cavallium.dbengine.database.collections.DatabaseLong;
import it.cavallium.dbengine.database.collections.DatabaseSingleton;
import it.cavallium.dbengine.database.serialization.Serializer;
import java.io.IOException;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

public abstract class TestSingletons {

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
		ensureNoLeaks(false, false);
	}

	@AfterEach
	public void afterEach() {
		ensureNoLeaks(true, false);
	}

	@Test
	public void testCreateInteger() throws IOException {
		tempDb(getTempDbGenerator(), db -> tempInt(db, "test", 0).get(null));
	}

	@Test
	public void testCreateIntegerNoop() throws IOException {
		tempDb(getTempDbGenerator(), db -> tempInt(db, "test", 0));
	}

	@Test
	public void testCreateLong() throws IOException {
		tempDb(getTempDbGenerator(), db -> tempLong(db, "test", 0).get(null));
	}

	@Test
	public void testCreateSingleton() throws IOException {
		tempDb(getTempDbGenerator(), db -> tempSingleton(db, "testsingleton").get(null));
	}

	@ParameterizedTest
	@ValueSource(ints = {Integer.MIN_VALUE, -192, -2, -1, 0, 1, 2, 1292, Integer.MAX_VALUE})
	public void testDefaultValueInteger(int i) throws IOException {
		Assertions.assertEquals((Integer) i, tempDb(getTempDbGenerator(), db -> tempInt(db, "test", i).get(null)));
	}

	@ParameterizedTest
	@ValueSource(longs = {Long.MIN_VALUE, -192, -2, -1, 0, 1, 2, 1292, Long.MAX_VALUE})
	public void testDefaultValueLong(long i) throws IOException {
		Assertions.assertEquals((Long) i, tempDb(getTempDbGenerator(), db -> tempLong(db, "test", i).get(null)));
	}

	@ParameterizedTest
	@MethodSource("provideNumberWithRepeats")
	public void testSetInteger(Integer i, Integer repeats) throws IOException {
		Assertions.assertEquals(i, tempDb(getTempDbGenerator(), db -> {
			var dbInt = tempInt(db, "test", 0);
			for (int integer = 0; integer < repeats; integer++) {
				dbInt.set((int) System.currentTimeMillis());
			}
			dbInt.set(i);
			return dbInt.get(null);
		}));
	}

	@ParameterizedTest
	@MethodSource("provideLongNumberWithRepeats")
	public void testSetLong(Long i, Integer repeats) throws IOException {
		Assertions.assertEquals(i, tempDb(getTempDbGenerator(), db -> {
			var dbLong = tempLong(db, "test", 0);
			for (int integer = 0; integer < repeats; integer++) {
				dbLong.set(System.currentTimeMillis());
			}
			dbLong.set(i);
			return dbLong.get(null);
		}));
	}

	@ParameterizedTest
	@MethodSource("provideLongNumberWithRepeats")
	public void testSetSingleton(Long i, Integer repeats) throws IOException {
		Assertions.assertEquals(Long.toString(i), tempDb(getTempDbGenerator(), db -> {
			var dbSingleton = tempSingleton(db, "test");
			for (int integer = 0; integer < repeats; integer++) {
				dbSingleton.set(Long.toString(System.currentTimeMillis()));
			}
			dbSingleton.set(Long.toString(i));
			return dbSingleton.get(null);
		}));
	}

	public static DatabaseInt tempInt(LLKeyValueDatabase database, String name, int defaultValue) {
		return database.getInteger("ints", name, defaultValue);
	}

	public static DatabaseLong tempLong(LLKeyValueDatabase database, String name, long defaultValue) {
		return database.getLong("longs", name, defaultValue);
	}

	public static DatabaseSingleton<String> tempSingleton(LLKeyValueDatabase database, String name) {
		return new DatabaseSingleton<>(database.getSingleton("longs", name), Serializer.UTF8_SERIALIZER);
	}
}
