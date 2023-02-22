package it.cavallium.dbengine.tests;

import static it.cavallium.dbengine.tests.DbTestUtils.ensureNoLeaks;
import static it.cavallium.dbengine.tests.DbTestUtils.runVoid;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import it.cavallium.dbengine.tests.DbTestUtils.TempDb;
import it.cavallium.dbengine.buffers.Buf;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.LLDictionaryResultType;
import it.cavallium.dbengine.database.LLKeyValueDatabase;
import it.cavallium.dbengine.database.UpdateMode;
import it.cavallium.dbengine.database.UpdateReturnMode;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Stream;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public abstract class TestLLDictionaryLeaks {

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
		var b = Buf.create(sb.length);
		b.addElements(0, sb);
		assert b.size() == sb.length;
		return b;
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
	}

	@ParameterizedTest
	@MethodSource("provideArguments")
	public void testGetColumnName(UpdateMode updateMode) {
		var dict = getDict(updateMode);
		dict.getColumnName();
	}

	@ParameterizedTest
	@MethodSource("provideArguments")
	public void testGet(UpdateMode updateMode) {
		var dict = getDict(updateMode);
		var key = fromString("test");
		dict.get(null, key);
		dict.get(null, key);
		dict.get(null, key);
	}

	@ParameterizedTest
	@MethodSource("providePutArguments")
	public void testPut(UpdateMode updateMode, LLDictionaryResultType resultType) {
		var dict = getDict(updateMode);
		var key = fromString("test-key");
		var value = fromString("test-value");
		dict.put(key, value, resultType);
	}

	@ParameterizedTest
	@MethodSource("provideArguments")
	public void testGetUpdateMode(UpdateMode updateMode) {
		var dict = getDict(updateMode);
		assertEquals(updateMode, dict.getUpdateMode());
	}

	@ParameterizedTest
	@MethodSource("provideUpdateArguments")
	public void testUpdate(UpdateMode updateMode, UpdateReturnMode updateReturnMode) {
		var dict = getDict(updateMode);
		var key = fromString("test-key");
		runVoid(updateMode == UpdateMode.DISALLOW, () -> dict.update(key, this::pass, updateReturnMode));
		runVoid(updateMode == UpdateMode.DISALLOW, () -> dict.update(key, this::pass, updateReturnMode));
		runVoid(updateMode == UpdateMode.DISALLOW, () -> dict.update(key, this::pass, updateReturnMode));
	}

	private Buf pass(@Nullable Buf old) {
		if (old == null) return null;
		return old.copy();
	}

	@ParameterizedTest
	@MethodSource("provideArguments")
	public void testUpdateAndGetDelta(UpdateMode updateMode) {
		var dict = getDict(updateMode);
		var key = fromString("test-key");
		runVoid(updateMode == UpdateMode.DISALLOW, () -> dict.updateAndGetDelta(key, this::pass));
		runVoid(updateMode == UpdateMode.DISALLOW, () -> dict.updateAndGetDelta(key, this::pass));
		runVoid(updateMode == UpdateMode.DISALLOW, () -> dict.updateAndGetDelta(key, this::pass));
	}

	@ParameterizedTest
	@MethodSource("provideArguments")
	public void testClear(UpdateMode updateMode) {
		var dict = getDict(updateMode);
		dict.clear();
	}

	@ParameterizedTest
	@MethodSource("providePutArguments")
	public void testRemove(UpdateMode updateMode, LLDictionaryResultType resultType) {
		var dict = getDict(updateMode);
		var key = fromString("test-key");
		dict.remove(key, resultType);
	}
}
