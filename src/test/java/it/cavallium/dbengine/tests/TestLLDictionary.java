package it.cavallium.dbengine.tests;

import static it.cavallium.dbengine.tests.DbTestUtils.ensureNoLeaks;
import static it.cavallium.dbengine.tests.DbTestUtils.runVoid;
import static it.cavallium.dbengine.utils.StreamUtils.toListClose;
import static org.junit.jupiter.api.Assertions.assertEquals;

import it.cavallium.dbengine.tests.DbTestUtils.TempDb;
import it.cavallium.dbengine.buffers.Buf;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.LLDictionaryResultType;
import it.cavallium.dbengine.database.LLKeyValueDatabase;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.UpdateMode;
import it.cavallium.dbengine.database.UpdateReturnMode;
import it.cavallium.dbengine.utils.StreamUtils;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
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

public abstract class TestLLDictionary {

	private final Logger log = LogManager.getLogger(this.getClass());
	private static final LLRange RANGE_ALL = LLRange.all();
	private TempDb tempDb;
	private LLKeyValueDatabase db;

	protected abstract TemporaryDbGenerator getTempDbGenerator();

	@BeforeEach
	public void beforeEach() throws IOException {
		ensureNoLeaks(false, false);
		tempDb = Objects.requireNonNull(getTempDbGenerator().openTempDb(), "TempDB");
		db = tempDb.db();
	}

	@AfterEach
	public void afterEach() throws IOException {
		getTempDbGenerator().closeTempDb(tempDb);
		ensureNoLeaks(true, false);
	}

	public static Stream<Arguments> provideArguments() {
		return Arrays.stream(UpdateMode.values()).map(Arguments::of);
	}

	public static Stream<Arguments> providePutArguments() {
		var updateModes = Arrays.stream(UpdateMode.values());
		return updateModes.flatMap(updateMode -> {
			var resultTypes = Arrays.stream(LLDictionaryResultType.values());
			return resultTypes.map(resultType -> Arguments.of(updateMode, resultType));
		});
	}

	public static Stream<Arguments> provideUpdateArguments() {
		var updateModes = Arrays.stream(UpdateMode.values());
		return updateModes.flatMap(updateMode -> {
			var resultTypes = Arrays.stream(UpdateReturnMode.values());
			return resultTypes.map(resultType -> Arguments.of(updateMode, resultType));
		});
	}

	private LLDictionary getDict(UpdateMode updateMode) {
		var dict = DbTestUtils.tempDictionary(db, updateMode);
		var key1 = fromString("test-key-1");
		var key2 = fromString("test-key-2");
		var key3 = fromString("test-key-3");
		var key4 = fromString("test-key-4");
		var value = fromString("test-value");
		dict.put(key1, value, LLDictionaryResultType.VOID);
		dict.put(key2, value, LLDictionaryResultType.VOID);
		dict.put(key3, value, LLDictionaryResultType.VOID);
		dict.put(key4, value, LLDictionaryResultType.VOID);
		return dict;
	}

	private Buf fromString(String s) {
		var sb = s.getBytes(StandardCharsets.UTF_8);
		Buf b = Buf.create(sb.length + 3 + 13);
		b.addElements(0, sb);
		assert b.size() == sb.length;
		return b;
	}

	private String toString(Buf bb) {
		return bb != null ? bb.toString(StandardCharsets.UTF_8) : null;
	}

	@Test
	public void testNoOp() {
	}

	@Test
	public void testNoOpAllocation() {
		for (int i = 0; i < 10; i++) {
			var a = Buf.create(i * 512);
		}
	}

	@ParameterizedTest
	@MethodSource("provideArguments")
	public void testGetDict(UpdateMode updateMode) {
		var dict = getDict(updateMode);
		Assertions.assertNotNull(dict);
	}

	@ParameterizedTest
	@MethodSource("provideArguments")
	public void testGetColumnName(UpdateMode updateMode) {
		var dict = getDict(updateMode);
		Assertions.assertEquals("hash_map_testmap", dict.getColumnName());
	}

	@ParameterizedTest
	@MethodSource("provideArguments")
	public void testGet(UpdateMode updateMode) {
		var dict = getDict(updateMode);
		var keyEx = fromString("test-key-1");
		var keyNonEx = fromString("test-nonexistent");
		Assertions.assertEquals("test-value", toString(dict.get(null, keyEx)));
		Assertions.assertEquals("test-value", toString(dict.get(null, keyEx)));
		Assertions.assertEquals("test-value", toString(dict.get(null, keyEx)));
		Assertions.assertEquals((String) null, toString(dict.get(null, keyNonEx)));
		Assertions.assertEquals((String) null, toString(dict.get(null, keyNonEx)));
		Assertions.assertEquals((String) null, toString(dict.get(null, keyNonEx)));
	}

	@ParameterizedTest
	@MethodSource("providePutArguments")
	public void testPutExisting(UpdateMode updateMode, LLDictionaryResultType resultType) {
		var dict = getDict(updateMode);
		var keyEx = fromString("test-key-1");
		var value = fromString("test-value");

		var beforeSize = dict.sizeRange(null, RANGE_ALL, false);

		dict.put(keyEx, value, resultType);

		var afterSize = dict.sizeRange(null, RANGE_ALL, false);
		Assertions.assertEquals(0, afterSize - beforeSize);
	}

	@ParameterizedTest
	@MethodSource("providePutArguments")
	public void testPutNew(UpdateMode updateMode, LLDictionaryResultType resultType) {
		var dict = getDict(updateMode);
		var keyNonEx = fromString("test-nonexistent");
		var value = fromString("test-value");

		var beforeSize = dict.sizeRange(null, RANGE_ALL, false);

		dict.put(keyNonEx, value, resultType);

		var afterSize = dict.sizeRange(null, LLRange.all(), false);
		Assertions.assertEquals(1, afterSize - beforeSize);

		Assertions.assertTrue(toListClose(dict.getRangeKeys(null, RANGE_ALL, false, false).map(this::toString)).contains("test-nonexistent"));
		Assertions.assertTrue(toListClose(dict.getRangeKeys(null, RANGE_ALL, true, false).map(this::toString)).contains("test-nonexistent"));
	}

	@ParameterizedTest
	@MethodSource("provideArguments")
	public void testGetUpdateMode(UpdateMode updateMode) {
		var dict = getDict(updateMode);
		assertEquals(updateMode, dict.getUpdateMode());
	}

	@ParameterizedTest
	@MethodSource("provideUpdateArguments")
	public void testUpdateExisting(UpdateMode updateMode, UpdateReturnMode updateReturnMode) {
		var dict = getDict(updateMode);
		var keyEx = fromString("test-key-1");
		var beforeSize = dict.sizeRange(null, RANGE_ALL, false);
		long afterSize;
		runVoid(updateMode == UpdateMode.DISALLOW, () -> dict.update(keyEx, old -> fromString("test-value"), updateReturnMode));
		afterSize = dict.sizeRange(null, RANGE_ALL, false);
		assertEquals(0, afterSize - beforeSize);
		runVoid(updateMode == UpdateMode.DISALLOW, () -> dict.update(keyEx, old -> fromString("test-value"), updateReturnMode));
		afterSize = dict.sizeRange(null, RANGE_ALL, false);
		assertEquals(0, afterSize - beforeSize);
		runVoid(updateMode == UpdateMode.DISALLOW, () -> dict.update(keyEx, old -> fromString("test-value"), updateReturnMode));
		afterSize = dict.sizeRange(null, RANGE_ALL, false);
		assertEquals(0, afterSize - beforeSize);
	}

	@ParameterizedTest
	@MethodSource("provideUpdateArguments")
	public void testUpdateNew(UpdateMode updateMode, UpdateReturnMode updateReturnMode) {
		int expected = updateMode == UpdateMode.DISALLOW ? 0 : 1;
		var dict = getDict(updateMode);
		var keyNonEx = fromString("test-nonexistent");
		var beforeSize = dict.sizeRange(null, RANGE_ALL, false);
		long afterSize;
		runVoid(updateMode == UpdateMode.DISALLOW, () -> dict.update(keyNonEx, old -> fromString("test-value"), updateReturnMode));
		afterSize = dict.sizeRange(null, RANGE_ALL, false);
		assertEquals(expected, afterSize - beforeSize);
		runVoid(updateMode == UpdateMode.DISALLOW, () -> dict.update(keyNonEx, old -> fromString("test-value"), updateReturnMode));
		afterSize = dict.sizeRange(null, RANGE_ALL, false);
		assertEquals(expected, afterSize - beforeSize);
		runVoid(updateMode == UpdateMode.DISALLOW, () -> dict.update(keyNonEx, old -> fromString("test-value"), updateReturnMode));
		afterSize = dict.sizeRange(null, RANGE_ALL, false);
		assertEquals(expected, afterSize - beforeSize);

		if (updateMode != UpdateMode.DISALLOW) {
			Assertions.assertTrue(toListClose(dict
					.getRangeKeys(null, RANGE_ALL, false, false)
					.map(this::toString))
					.contains("test-nonexistent"));
			Assertions.assertTrue(toListClose(dict
					.getRangeKeys(null, RANGE_ALL, true, false)
					.map(this::toString))
					.contains("test-nonexistent"));
		}
	}

	@ParameterizedTest
	@MethodSource("provideArguments")
	public void testUpdateAndGetDelta(UpdateMode updateMode) {
		log.warn("Test not implemented");
		//todo: implement
	}

	@ParameterizedTest
	@MethodSource("provideArguments")
	public void testClear(UpdateMode updateMode) {
		log.warn("Test not implemented");
		//todo: implement
	}

	@ParameterizedTest
	@MethodSource("providePutArguments")
	public void testRemove(UpdateMode updateMode, LLDictionaryResultType resultType) {
		log.warn("Test not implemented");
		//todo: implement
	}
}
