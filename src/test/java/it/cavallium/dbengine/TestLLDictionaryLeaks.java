package it.cavallium.dbengine;

import static it.cavallium.dbengine.DbTestUtils.destroyAllocator;
import static it.cavallium.dbengine.DbTestUtils.ensureNoLeaks;
import static it.cavallium.dbengine.DbTestUtils.newAllocator;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.netty5.buffer.api.Buffer;
import io.netty5.buffer.api.Send;
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
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class TestLLDictionaryLeaks {

	private TestAllocator allocator;
	private TempDb tempDb;
	private LLKeyValueDatabase db;

	@BeforeEach
	public void beforeEach() {
		this.allocator = newAllocator();
		ensureNoLeaks(allocator.allocator(), false);
		tempDb = Objects.requireNonNull(DbTestUtils.openTempDb(allocator).block(), "TempDB");
		db = tempDb.db();
	}

	@AfterEach
	public void afterEach() {
		DbTestUtils.closeTempDb(tempDb).block();
		ensureNoLeaks(allocator.allocator(), true);
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
			b.writeBytes(b);
			return b.send();
		}
	}

	private void run(Flux<?> publisher) {
		publisher.subscribeOn(Schedulers.immediate()).blockLast();
	}

	private void runVoid(Mono<Void> publisher) {
		publisher.then().subscribeOn(Schedulers.immediate()).block();
	}

	private <T> T run(Mono<T> publisher) {
		return publisher.subscribeOn(Schedulers.immediate()).block();
	}

	private <T> T run(boolean shouldFail, Mono<T> publisher) {
		return publisher.subscribeOn(Schedulers.immediate()).transform(mono -> {
			if (shouldFail) {
				return mono.onErrorResume(ex -> Mono.empty());
			} else {
				return mono;
			}
		}).block();
	}

	private void runVoid(boolean shouldFail, Mono<Void> publisher) {
		publisher.then().subscribeOn(Schedulers.immediate()).transform(mono -> {
			if (shouldFail) {
				return mono.onErrorResume(ex -> Mono.empty());
			} else {
				return mono;
			}
		}).block();
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
		runVoid(dict.get(null, key).then().transform(LLUtils::handleDiscard));
		runVoid(dict.get(null, key, true).then().transform(LLUtils::handleDiscard));
		runVoid(dict.get(null, key, false).then().transform(LLUtils::handleDiscard));
	}

	@ParameterizedTest
	@MethodSource("providePutArguments")
	public void testPut(UpdateMode updateMode, LLDictionaryResultType resultType) {
		var dict = getDict(updateMode);
		var key = Mono.fromCallable(() -> fromString("test-key"));
		var value = Mono.fromCallable(() -> fromString("test-value"));
		runVoid(dict.put(key, value, resultType).then());
	}

	@ParameterizedTest
	@MethodSource("provideArguments")
	public void testGetUpdateMode(UpdateMode updateMode) {
		var dict = getDict(updateMode);
		assertEquals(updateMode, run(dict.getUpdateMode()));
	}

	@ParameterizedTest
	@MethodSource("provideUpdateArguments")
	public void testUpdate(UpdateMode updateMode, UpdateReturnMode updateReturnMode) {
		var dict = getDict(updateMode);
		var key = Mono.fromCallable(() -> fromString("test-key"));
		runVoid(updateMode == UpdateMode.DISALLOW,
				dict.update(key, old -> old, updateReturnMode, true).then().transform(LLUtils::handleDiscard)
		);
		runVoid(updateMode == UpdateMode.DISALLOW,
				dict.update(key, old -> old, updateReturnMode, false).then().transform(LLUtils::handleDiscard)
		);
		runVoid(updateMode == UpdateMode.DISALLOW,
				dict.update(key, old -> old, updateReturnMode).then().transform(LLUtils::handleDiscard)
		);
	}

	@ParameterizedTest
	@MethodSource("provideArguments")
	public void testUpdateAndGetDelta(UpdateMode updateMode) {
		var dict = getDict(updateMode);
		var key = Mono.fromCallable(() -> fromString("test-key"));
		runVoid(updateMode == UpdateMode.DISALLOW,
				dict.updateAndGetDelta(key, old -> old, true).then().transform(LLUtils::handleDiscard)
		);
		runVoid(updateMode == UpdateMode.DISALLOW,
				dict.updateAndGetDelta(key, old -> old, false).then().transform(LLUtils::handleDiscard)
		);
		runVoid(updateMode == UpdateMode.DISALLOW,
				dict.updateAndGetDelta(key, old -> old).then().transform(LLUtils::handleDiscard)
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
		runVoid(dict.remove(key, resultType).then());
	}
}
