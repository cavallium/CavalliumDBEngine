package it.cavallium.dbengine.database;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import it.cavallium.dbengine.client.BadBlock;
import it.cavallium.dbengine.database.serialization.BiSerializationFunction;
import it.cavallium.dbengine.database.serialization.SerializationFunction;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.jetbrains.annotations.Nullable;
import org.warp.commonutils.concurrency.atomicity.NotAtomic;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuple3;

@SuppressWarnings("unused")
@NotAtomic
public interface LLDictionary extends LLKeyValueDatabaseStructure {

	String getColumnName();

	ByteBufAllocator getAllocator();

	Mono<ByteBuf> get(@Nullable LLSnapshot snapshot, Mono<ByteBuf> key, boolean existsAlmostCertainly);

	default Mono<ByteBuf> get(@Nullable LLSnapshot snapshot, Mono<ByteBuf> key) {
		return get(snapshot, key, false);
	}

	Mono<ByteBuf> put(Mono<ByteBuf> key, Mono<ByteBuf> value, LLDictionaryResultType resultType);

	Mono<UpdateMode> getUpdateMode();

	default Mono<ByteBuf> update(Mono<ByteBuf> key,
			SerializationFunction<@Nullable ByteBuf, @Nullable ByteBuf> updater,
			UpdateReturnMode updateReturnMode,
			boolean existsAlmostCertainly) {
		return this
				.updateAndGetDelta(key, updater, existsAlmostCertainly)
				.transform(prev -> LLUtils.resolveDelta(prev, updateReturnMode));
	}

	default Mono<ByteBuf> update(Mono<ByteBuf> key,
			SerializationFunction<@Nullable ByteBuf, @Nullable ByteBuf> updater,
			UpdateReturnMode returnMode) {
		return update(key, updater, returnMode, false);
	}

	Mono<Delta<ByteBuf>> updateAndGetDelta(Mono<ByteBuf> key,
			SerializationFunction<@Nullable ByteBuf, @Nullable ByteBuf> updater,
			boolean existsAlmostCertainly);

	default Mono<Delta<ByteBuf>> updateAndGetDelta(Mono<ByteBuf> key,
			SerializationFunction<@Nullable ByteBuf, @Nullable ByteBuf> updater) {
		return updateAndGetDelta(key, updater, false);
	}

	Mono<Void> clear();

	Mono<ByteBuf> remove(Mono<ByteBuf> key, LLDictionaryResultType resultType);

	<K> Flux<Tuple3<K, ByteBuf, Optional<ByteBuf>>> getMulti(@Nullable LLSnapshot snapshot,
			Flux<Tuple2<K, ByteBuf>> keys,
			boolean existsAlmostCertainly);

	default <K> Flux<Tuple3<K, ByteBuf, Optional<ByteBuf>>> getMulti(@Nullable LLSnapshot snapshot, Flux<Tuple2<K, ByteBuf>> keys) {
		return getMulti(snapshot, keys, false);
	}

	Flux<Entry<ByteBuf, ByteBuf>> putMulti(Flux<Entry<ByteBuf, ByteBuf>> entries, boolean getOldValues);

	<X> Flux<ExtraKeyOperationResult<ByteBuf, X>> updateMulti(Flux<Tuple2<ByteBuf, X>> entries,
			BiSerializationFunction<ByteBuf, X, ByteBuf> updateFunction);

	Flux<Entry<ByteBuf, ByteBuf>> getRange(@Nullable LLSnapshot snapshot, Mono<LLRange> range, boolean existsAlmostCertainly);

	default Flux<Entry<ByteBuf, ByteBuf>> getRange(@Nullable LLSnapshot snapshot, Mono<LLRange> range) {
		return getRange(snapshot, range, false);
	}

	Flux<List<Entry<ByteBuf, ByteBuf>>> getRangeGrouped(@Nullable LLSnapshot snapshot,
			Mono<LLRange> range,
			int prefixLength,
			boolean existsAlmostCertainly);

	default Flux<List<Entry<ByteBuf, ByteBuf>>> getRangeGrouped(@Nullable LLSnapshot snapshot,
			Mono<LLRange> range,
			int prefixLength) {
		return getRangeGrouped(snapshot, range, prefixLength, false);
	}

	Flux<ByteBuf> getRangeKeys(@Nullable LLSnapshot snapshot, Mono<LLRange> range);

	Flux<List<ByteBuf>> getRangeKeysGrouped(@Nullable LLSnapshot snapshot, Mono<LLRange> range, int prefixLength);

	Flux<ByteBuf> getRangeKeyPrefixes(@Nullable LLSnapshot snapshot, Mono<LLRange> range, int prefixLength);

	Flux<BadBlock> badBlocks(Mono<LLRange> range);

	Mono<Void> setRange(Mono<LLRange> range, Flux<Entry<ByteBuf, ByteBuf>> entries);

	default Mono<Void> replaceRange(Mono<LLRange> range,
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

	default Mono<Void> replaceRange(Mono<LLRange> range,
			boolean canKeysChange,
			Function<Entry<ByteBuf, ByteBuf>, Mono<Entry<ByteBuf, ByteBuf>>> entriesReplacer) {
		return replaceRange(range, canKeysChange, entriesReplacer, false);
	}

	Mono<Boolean> isRangeEmpty(@Nullable LLSnapshot snapshot, Mono<LLRange> range);

	Mono<Long> sizeRange(@Nullable LLSnapshot snapshot, Mono<LLRange> range, boolean fast);

	Mono<Entry<ByteBuf, ByteBuf>> getOne(@Nullable LLSnapshot snapshot, Mono<LLRange> range);

	Mono<ByteBuf> getOneKey(@Nullable LLSnapshot snapshot, Mono<LLRange> range);

	Mono<Entry<ByteBuf, ByteBuf>> removeOne(Mono<LLRange> range);
}
