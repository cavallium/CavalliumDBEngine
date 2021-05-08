package it.cavallium.dbengine.database;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.util.List;
import java.util.Map.Entry;
import java.util.function.Function;
import org.jetbrains.annotations.Nullable;
import org.warp.commonutils.concurrency.atomicity.NotAtomic;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

@SuppressWarnings("unused")
@NotAtomic
public interface LLDictionary extends LLKeyValueDatabaseStructure {

	ByteBufAllocator getAllocator();

	Mono<ByteBuf> get(@Nullable LLSnapshot snapshot, ByteBuf key, boolean existsAlmostCertainly);

	default Mono<ByteBuf> get(@Nullable LLSnapshot snapshot, ByteBuf key) {
		return get(snapshot, key, false);
	}

	Mono<ByteBuf> put(ByteBuf key, ByteBuf value, LLDictionaryResultType resultType);

	Mono<UpdateMode> getUpdateMode();

	default Mono<ByteBuf> update(ByteBuf key,
			Function<@Nullable ByteBuf, @Nullable ByteBuf> updater,
			UpdateReturnMode updateReturnMode,
			boolean existsAlmostCertainly) {
		return this
				.updateAndGetDelta(key, updater, existsAlmostCertainly)
				.transform(prev -> LLUtils.resolveDelta(prev, updateReturnMode));
	}

	default Mono<ByteBuf> update(ByteBuf key,
			Function<@Nullable ByteBuf, @Nullable ByteBuf> updater,
			UpdateReturnMode returnMode) {
		return update(key, updater, returnMode, false);
	}

	Mono<Delta<ByteBuf>> updateAndGetDelta(ByteBuf key,
			Function<@Nullable ByteBuf, @Nullable ByteBuf> updater,
			boolean existsAlmostCertainly);

	default Mono<Delta<ByteBuf>> updateAndGetDelta(ByteBuf key,
			Function<@Nullable ByteBuf, @Nullable ByteBuf> updater) {
		return updateAndGetDelta(key, updater, false);
	}

	Mono<Void> clear();

	Mono<ByteBuf> remove(ByteBuf key, LLDictionaryResultType resultType);

	Flux<Entry<ByteBuf, ByteBuf>> getMulti(@Nullable LLSnapshot snapshot, Flux<ByteBuf> keys, boolean existsAlmostCertainly);

	default Flux<Entry<ByteBuf, ByteBuf>> getMulti(@Nullable LLSnapshot snapshot, Flux<ByteBuf> keys) {
		return getMulti(snapshot, keys, false);
	}

	Flux<Entry<ByteBuf, ByteBuf>> putMulti(Flux<Entry<ByteBuf, ByteBuf>> entries, boolean getOldValues);

	Flux<Entry<ByteBuf, ByteBuf>> getRange(@Nullable LLSnapshot snapshot, LLRange range, boolean existsAlmostCertainly);

	default Flux<Entry<ByteBuf, ByteBuf>> getRange(@Nullable LLSnapshot snapshot, LLRange range) {
		return getRange(snapshot, range, false);
	}

	Flux<List<Entry<ByteBuf, ByteBuf>>> getRangeGrouped(@Nullable LLSnapshot snapshot,
			LLRange range,
			int prefixLength,
			boolean existsAlmostCertainly);

	default Flux<List<Entry<ByteBuf, ByteBuf>>> getRangeGrouped(@Nullable LLSnapshot snapshot,
			LLRange range,
			int prefixLength) {
		return getRangeGrouped(snapshot, range, prefixLength, false);
	}

	Flux<ByteBuf> getRangeKeys(@Nullable LLSnapshot snapshot, LLRange range);

	Flux<List<ByteBuf>> getRangeKeysGrouped(@Nullable LLSnapshot snapshot, LLRange range, int prefixLength);

	Flux<ByteBuf> getRangeKeyPrefixes(@Nullable LLSnapshot snapshot, LLRange range, int prefixLength);

	Mono<Void> setRange(LLRange range, Flux<Entry<ByteBuf, ByteBuf>> entries);

	default Mono<Void> replaceRange(LLRange range,
			boolean canKeysChange,
			Function<Entry<ByteBuf, ByteBuf>, Mono<Entry<ByteBuf, ByteBuf>>> entriesReplacer,
			boolean existsAlmostCertainly) {
		return Mono.defer(() -> {
			if (canKeysChange) {
				return this
						.setRange(range, this
								.getRange(null, range, existsAlmostCertainly)
								.flatMap(entriesReplacer)
						);
			} else {
				return this
						.putMulti(this
								.getRange(null, range, existsAlmostCertainly)
								.flatMap(entriesReplacer), false)
						.then();
			}
		});
	}

	default Mono<Void> replaceRange(LLRange range,
			boolean canKeysChange,
			Function<Entry<ByteBuf, ByteBuf>, Mono<Entry<ByteBuf, ByteBuf>>> entriesReplacer) {
		return replaceRange(range, canKeysChange, entriesReplacer, false);
	}

	Mono<Boolean> isRangeEmpty(@Nullable LLSnapshot snapshot, LLRange range);

	Mono<Long> sizeRange(@Nullable LLSnapshot snapshot, LLRange range, boolean fast);

	Mono<Entry<ByteBuf, ByteBuf>> getOne(@Nullable LLSnapshot snapshot, LLRange range);

	Mono<ByteBuf> getOneKey(@Nullable LLSnapshot snapshot, LLRange range);

	Mono<Entry<ByteBuf, ByteBuf>> removeOne(LLRange range);
}
