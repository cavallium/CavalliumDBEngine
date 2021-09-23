package it.cavallium.dbengine;

import static it.cavallium.dbengine.DbTestUtils.destroyAllocator;
import static it.cavallium.dbengine.DbTestUtils.ensureNoLeaks;
import static it.cavallium.dbengine.DbTestUtils.newAllocator;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.net5.buffer.api.Buffer;
import io.net5.buffer.api.Send;
import it.cavallium.dbengine.DbTestUtils.TempDb;
import it.cavallium.dbengine.DbTestUtils.TestAllocator;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.LLDictionaryResultType;
import it.cavallium.dbengine.database.LLKeyValueDatabase;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.UpdateMode;
import it.cavallium.dbengine.database.UpdateReturnMode;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import reactor.core.publisher.Mono;

public abstract class TestLLDictionaryLeaks {

	private TestAllocator allocator;
	private TempDb tempDb;
	private LLKeyValueDatabase db;

	protected abstract TemporaryDbGenerator getTempDbGenerator();

	@BeforeEach
	public void beforeEach() {
		this.allocator = newAllocator();
		ensureNoLeaks(allocator.allocator(), false, false);
		tempDb = Objects.requireNonNull(getTempDbGenerator().openTempDb(allocator).block(), "TempDB");
		db = tempDb.db();
	}

	@AfterEach
	public void afterEach() {
		getTempDbGenerator().closeTempDb(tempDb).block();
		ensureNoLeaks(allocator.allocator(), true, false);
		destroyAllocator(allocator);
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
		var dict = DbTestUtils.tempDictionary(db, updateMode).blockOptional().orElseThrow();
		var key1 = Mono.fromCallable(() -> fromString("test-key-1"));
		var key2 = Mono.fromCallable(() -> fromString("test-key-2"));
		var key3 = Mono.fromCallable(() -> fromString("test-key-3"));
		var key4 = Mono.fromCallable(() -> fromString("test-key-4"));
		var value = Mono.fromCallable(() -> fromString("test-value"));
		dict.put(key1, value, LLDictionaryResultType.VOID).block();
		dict.put(key2, value, LLDictionaryResultType.VOID).block();
		dict.put(key3, value, LLDictionaryResultType.VOID).block();
		dict.put(key4, value, LLDictionaryResultType.VOID).block();
		return dict;
	}

	private Send<Buffer> fromString(String s) {
		var sb = s.getBytes(StandardCharsets.UTF_8);
		try (var b = db.getAllocator().allocate(sb.length)) {
			b.writeBytes(sb);
			assert b.readableBytes() == sb.length;
			return b.send();
		}
	}

	@Test
	public void testNoOp() {
	}

	@Test
	public void testNoOpAllocation() {
		for (int i = 0; i < 10; i++) {
			var a = allocator.allocator().allocate(i * 512);
			a.send().receive().close();
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
	public void testGetAllocator(UpdateMode updateMode) {
		var dict = getDict(updateMode);
		dict.getAllocator();
	}

	@ParameterizedTest
	@MethodSource("provideArguments")
	public void testGet(UpdateMode updateMode) {
		var dict = getDict(updateMode);
		var key = Mono.fromCallable(() -> fromString("test"));
		DbTestUtils.runVoid(dict.get(null, key).then().transform(LLUtils::handleDiscard));
		DbTestUtils.runVoid(dict.get(null, key, true).then().transform(LLUtils::handleDiscard));
		DbTestUtils.runVoid(dict.get(null, key, false).then().transform(LLUtils::handleDiscard));
	}

	@ParameterizedTest
	@MethodSource("providePutArguments")
	public void testPut(UpdateMode updateMode, LLDictionaryResultType resultType) {
		var dict = getDict(updateMode);
		var key = Mono.fromCallable(() -> fromString("test-key"));
		var value = Mono.fromCallable(() -> fromString("test-value"));
		DbTestUtils.runVoid(dict.put(key, value, resultType).then().doOnDiscard(Send.class, Send::close));
	}

	@ParameterizedTest
	@MethodSource("provideArguments")
	public void testGetUpdateMode(UpdateMode updateMode) {
		var dict = getDict(updateMode);
		assertEquals(updateMode, DbTestUtils.run(dict.getUpdateMode()));
	}

	@ParameterizedTest
	@MethodSource("provideUpdateArguments")
	public void testUpdate(UpdateMode updateMode, UpdateReturnMode updateReturnMode) {
		var dict = getDict(updateMode);
		var key = Mono.fromCallable(() -> fromString("test-key"));
		DbTestUtils.runVoid(updateMode == UpdateMode.DISALLOW,
				dict.update(key, old -> old, updateReturnMode, true).then().transform(LLUtils::handleDiscard)
		);
		DbTestUtils.runVoid(updateMode == UpdateMode.DISALLOW,
				dict.update(key, old -> old, updateReturnMode, false).then().transform(LLUtils::handleDiscard)
		);
		DbTestUtils.runVoid(updateMode == UpdateMode.DISALLOW,
				dict.update(key, old -> old, updateReturnMode).then().transform(LLUtils::handleDiscard)
		);
	}

	@ParameterizedTest
	@MethodSource("provideArguments")
	public void testUpdateAndGetDelta(UpdateMode updateMode) {
		var dict = getDict(updateMode);
		var key = Mono.fromCallable(() -> fromString("test-key"));
		DbTestUtils.runVoid(updateMode == UpdateMode.DISALLOW,
				dict.updateAndGetDelta(key, old -> old, true).then().transform(LLUtils::handleDiscard)
		);
		DbTestUtils.runVoid(updateMode == UpdateMode.DISALLOW,
				dict.updateAndGetDelta(key, old -> old, false).then().transform(LLUtils::handleDiscard)
		);
		DbTestUtils.runVoid(updateMode == UpdateMode.DISALLOW,
				dict.updateAndGetDelta(key, old -> old).then().transform(LLUtils::handleDiscard)
		);
	}

	@ParameterizedTest
	@MethodSource("provideArguments")
	public void testClear(UpdateMode updateMode) {
		var dict = getDict(updateMode);
		DbTestUtils.runVoid(dict.clear());
	}

	@ParameterizedTest
	@MethodSource("providePutArguments")
	public void testRemove(UpdateMode updateMode, LLDictionaryResultType resultType) {
		var dict = getDict(updateMode);
		var key = Mono.fromCallable(() -> fromString("test-key"));
		DbTestUtils.runVoid(dict.remove(key, resultType).then().doOnDiscard(Send.class, Send::close));
	}
}
