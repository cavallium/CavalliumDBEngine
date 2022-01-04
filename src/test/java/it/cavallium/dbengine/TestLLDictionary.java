package it.cavallium.dbengine;

import static it.cavallium.dbengine.DbTestUtils.destroyAllocator;
import static it.cavallium.dbengine.DbTestUtils.ensureNoLeaks;
import static it.cavallium.dbengine.DbTestUtils.newAllocator;
import static it.cavallium.dbengine.SyncUtils.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.net5.buffer.api.Buffer;
import io.net5.buffer.api.Send;
import it.cavallium.dbengine.DbTestUtils.TempDb;
import it.cavallium.dbengine.DbTestUtils.TestAllocator;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.LLDictionaryResultType;
import it.cavallium.dbengine.database.LLKeyValueDatabase;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.UpdateMode;
import it.cavallium.dbengine.database.UpdateReturnMode;
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
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public abstract class TestLLDictionary {

	private final Logger log = LogManager.getLogger(this.getClass());
	private static final Mono<Send<LLRange>> RANGE_ALL = Mono.fromCallable(() -> LLRange.all().send());
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
		var key1 = Mono.fromCallable(() -> fromString("test-key-1").send());
		var key2 = Mono.fromCallable(() -> fromString("test-key-2").send());
		var key3 = Mono.fromCallable(() -> fromString("test-key-3").send());
		var key4 = Mono.fromCallable(() -> fromString("test-key-4").send());
		var value = Mono.fromCallable(() -> fromString("test-value").send());
		dict.put(key1, value, LLDictionaryResultType.VOID).block();
		dict.put(key2, value, LLDictionaryResultType.VOID).block();
		dict.put(key3, value, LLDictionaryResultType.VOID).block();
		dict.put(key4, value, LLDictionaryResultType.VOID).block();
		return dict;
	}

	private Buffer fromString(String s) {
		var sb = s.getBytes(StandardCharsets.UTF_8);
		try (var b = db.getAllocator().allocate(sb.length + 3 + 13)) {
			assert b.writerOffset() == 0;
			assert b.readerOffset() == 0;
			b.writerOffset(3).writeBytes(sb);
			b.readerOffset(3);
			assert b.readableBytes() == sb.length;

			var part1 = b.split();

			return LLUtils.compositeBuffer(db.getAllocator(), part1.send(), b.send());
		}
	}

	private String toString(Send<Buffer> b) {
		try (var bb = b.receive()) {
			byte[] data = new byte[bb.readableBytes()];
			bb.copyInto(bb.readerOffset(), data, 0, data.length);
			return new String(data, StandardCharsets.UTF_8);
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
	public void testGetAllocator(UpdateMode updateMode) {
		var dict = getDict(updateMode);
		var alloc = dict.getAllocator();
		Assertions.assertEquals(alloc, alloc);
	}

	@ParameterizedTest
	@MethodSource("provideArguments")
	public void testGet(UpdateMode updateMode) {
		var dict = getDict(updateMode);
		var keyEx = Mono.fromCallable(() -> fromString("test-key-1").send());
		var keyNonEx = Mono.fromCallable(() -> fromString("test-nonexistent").send());
		Assertions.assertEquals("test-value", run(dict.get(null, keyEx).map(this::toString).transform(LLUtils::handleDiscard)));
		Assertions.assertEquals("test-value", run(dict.get(null, keyEx, true).map(this::toString).transform(LLUtils::handleDiscard)));
		Assertions.assertEquals("test-value", run(dict.get(null, keyEx, false).map(this::toString).transform(LLUtils::handleDiscard)));
		Assertions.assertEquals((String) null, run(dict.get(null, keyNonEx).map(this::toString).transform(LLUtils::handleDiscard)));
		Assertions.assertEquals((String) null, run(dict.get(null, keyNonEx, true).map(this::toString).transform(LLUtils::handleDiscard)));
		Assertions.assertEquals((String) null, run(dict.get(null, keyNonEx, false).map(this::toString).transform(LLUtils::handleDiscard)));
	}

	@ParameterizedTest
	@MethodSource("providePutArguments")
	public void testPutExisting(UpdateMode updateMode, LLDictionaryResultType resultType) {
		var dict = getDict(updateMode);
		var keyEx = Mono.fromCallable(() -> fromString("test-key-1").send());
		var value = Mono.fromCallable(() -> fromString("test-value").send());

		var beforeSize = run(dict.sizeRange(null, RANGE_ALL, false));

		runVoid(dict.put(keyEx, value, resultType).then().doOnDiscard(Send.class, Send::close));

		var afterSize = run(dict.sizeRange(null, RANGE_ALL, false));
		Assertions.assertEquals(0, afterSize - beforeSize);
	}

	@ParameterizedTest
	@MethodSource("providePutArguments")
	public void testPutNew(UpdateMode updateMode, LLDictionaryResultType resultType) {
		var dict = getDict(updateMode);
		var keyNonEx = Mono.fromCallable(() -> fromString("test-nonexistent").send());
		var value = Mono.fromCallable(() -> fromString("test-value").send());

		var beforeSize = run(dict.sizeRange(null, RANGE_ALL, false));

		runVoid(dict.put(keyNonEx, value, resultType).then().doOnDiscard(Send.class, Send::close));

		var afterSize = run(dict.sizeRange(null, Mono.fromCallable(() -> LLRange.all().send()), false));
		Assertions.assertEquals(1, afterSize - beforeSize);

		Assertions.assertTrue(run(dict.getRangeKeys(null, RANGE_ALL).map(this::toString).collectList()).contains("test-nonexistent"));
	}

	@ParameterizedTest
	@MethodSource("provideArguments")
	public void testGetUpdateMode(UpdateMode updateMode) {
		var dict = getDict(updateMode);
		assertEquals(updateMode, run(dict.getUpdateMode()));
	}

	@ParameterizedTest
	@MethodSource("provideUpdateArguments")
	public void testUpdateExisting(UpdateMode updateMode, UpdateReturnMode updateReturnMode) {
		var dict = getDict(updateMode);
		var keyEx = Mono.fromCallable(() -> fromString("test-key-1").send());
		var beforeSize = run(dict.sizeRange(null, RANGE_ALL, false));
		long afterSize;
		runVoid(updateMode == UpdateMode.DISALLOW,
				dict.update(keyEx, old -> fromString("test-value"), updateReturnMode, true).then().transform(LLUtils::handleDiscard)
		);
		afterSize = run(dict.sizeRange(null, RANGE_ALL, false));
		assertEquals(0, afterSize - beforeSize);
		runVoid(updateMode == UpdateMode.DISALLOW,
				dict.update(keyEx, old -> fromString("test-value"), updateReturnMode, false).then().transform(LLUtils::handleDiscard)
		);
		afterSize = run(dict.sizeRange(null, RANGE_ALL, false));
		assertEquals(0, afterSize - beforeSize);
		runVoid(updateMode == UpdateMode.DISALLOW,
				dict.update(keyEx, old -> fromString("test-value"), updateReturnMode).then().transform(LLUtils::handleDiscard)
		);
		afterSize = run(dict.sizeRange(null, RANGE_ALL, false));
		assertEquals(0, afterSize - beforeSize);
	}

	@ParameterizedTest
	@MethodSource("provideUpdateArguments")
	public void testUpdateNew(UpdateMode updateMode, UpdateReturnMode updateReturnMode) {
		int expected = updateMode == UpdateMode.DISALLOW ? 0 : 1;
		var dict = getDict(updateMode);
		var keyNonEx = Mono.fromCallable(() -> fromString("test-nonexistent").send());
		var beforeSize = run(dict.sizeRange(null, RANGE_ALL, false));
		long afterSize;
		runVoid(updateMode == UpdateMode.DISALLOW,
				dict.update(keyNonEx, old -> fromString("test-value"), updateReturnMode, true).then().transform(LLUtils::handleDiscard)
		);
		afterSize = run(dict.sizeRange(null, RANGE_ALL, false));
		assertEquals(expected, afterSize - beforeSize);
		runVoid(updateMode == UpdateMode.DISALLOW,
				dict.update(keyNonEx, old -> fromString("test-value"), updateReturnMode, false).then().transform(LLUtils::handleDiscard)
		);
		afterSize = run(dict.sizeRange(null, RANGE_ALL, false));
		assertEquals(expected, afterSize - beforeSize);
		runVoid(updateMode == UpdateMode.DISALLOW,
				dict.update(keyNonEx, old -> fromString("test-value"), updateReturnMode).then().transform(LLUtils::handleDiscard)
		);
		afterSize = run(dict.sizeRange(null, RANGE_ALL, false));
		assertEquals(expected, afterSize - beforeSize);

		if (updateMode != UpdateMode.DISALLOW) {
			Assertions.assertTrue(run(dict.getRangeKeys(null, RANGE_ALL).map(this::toString).collectList()).contains(
					"test-nonexistent"));
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
