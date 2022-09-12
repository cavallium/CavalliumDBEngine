package it.cavallium.dbengine;

import static it.cavallium.dbengine.DbTestUtils.destroyAllocator;
import static it.cavallium.dbengine.DbTestUtils.ensureNoLeaks;
import static it.cavallium.dbengine.DbTestUtils.newAllocator;
import static it.cavallium.dbengine.SyncUtils.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.netty5.buffer.api.Buffer;
import io.netty5.util.Send;
import it.cavallium.dbengine.DbTestUtils.TempDb;
import it.cavallium.dbengine.DbTestUtils.TestAllocator;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.LLDictionaryResultType;
import it.cavallium.dbengine.database.LLKeyValueDatabase;
import it.cavallium.dbengine.database.UpdateMode;
import it.cavallium.dbengine.database.UpdateReturnMode;
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

	private Buffer fromString(String s) {
		var sb = s.getBytes(StandardCharsets.UTF_8);
		var b = db.getAllocator().allocate(sb.length);
		try {
			b.writeBytes(sb);
			assert b.readableBytes() == sb.length;
			return b;
		} catch (Throwable ex) {
			b.close();
			throw ex;
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
		runVoid(dict.get(null, key).then());
		runVoid(dict.get(null, key).then());
		runVoid(dict.get(null, key).then());
	}

	@ParameterizedTest
	@MethodSource("providePutArguments")
	public void testPut(UpdateMode updateMode, LLDictionaryResultType resultType) {
		var dict = getDict(updateMode);
		var key = Mono.fromCallable(() -> fromString("test-key"));
		var value = Mono.fromCallable(() -> fromString("test-value"));
		runVoid(dict.put(key, value, resultType).then().doOnDiscard(Buffer.class, Buffer::close));
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
		var key = Mono.fromCallable(() -> fromString("test-key"));
		runVoid(updateMode == UpdateMode.DISALLOW,
				dict.update(key, this::pass, updateReturnMode).then()
		);
		runVoid(updateMode == UpdateMode.DISALLOW,
				dict.update(key, this::pass, updateReturnMode).then()
		);
		runVoid(updateMode == UpdateMode.DISALLOW,
				dict.update(key, this::pass, updateReturnMode).then()
		);
	}

	private Buffer pass(@Nullable Buffer old) {
		if (old == null) return null;
		return old.copy();
	}

	@ParameterizedTest
	@MethodSource("provideArguments")
	public void testUpdateAndGetDelta(UpdateMode updateMode) {
		var dict = getDict(updateMode);
		var key = Mono.fromCallable(() -> fromString("test-key"));
		runVoid(updateMode == UpdateMode.DISALLOW,
				dict.updateAndGetDelta(key, this::pass).then()
		);
		runVoid(updateMode == UpdateMode.DISALLOW,
				dict.updateAndGetDelta(key, this::pass).then()
		);
		runVoid(updateMode == UpdateMode.DISALLOW,
				dict.updateAndGetDelta(key, this::pass).then()
		);
	}

	@ParameterizedTest
	@MethodSource("provideArguments")
	public void testClear(UpdateMode updateMode) {
		var dict = getDict(updateMode);
		runVoid(dict.clear());
	}

	@ParameterizedTest
	@MethodSource("providePutArguments")
	public void testRemove(UpdateMode updateMode, LLDictionaryResultType resultType) {
		var dict = getDict(updateMode);
		var key = Mono.fromCallable(() -> fromString("test-key"));
		runVoid(dict.remove(key, resultType).then().doOnDiscard(Buffer.class, Buffer::close));
	}
}
