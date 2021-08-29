package it.cavallium.dbengine.database;

import io.netty.buffer.api.Buffer;
import io.netty.buffer.api.BufferAllocator;
import io.netty.buffer.api.Send;
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

	BufferAllocator getAllocator();

	Mono<Send<Buffer>> get(@Nullable LLSnapshot snapshot, Mono<Send<Buffer>> key, boolean existsAlmostCertainly);

	default Mono<Send<Buffer>> get(@Nullable LLSnapshot snapshot, Mono<Send<Buffer>> key) {
		return get(snapshot, key, false);
	}

	Mono<Send<Buffer>> put(Mono<Send<Buffer>> key, Mono<Send<Buffer>> value, LLDictionaryResultType resultType);

	Mono<UpdateMode> getUpdateMode();

	default Mono<Send<Buffer>> update(Mono<Send<Buffer>> key,
			SerializationFunction<@Nullable Send<Buffer>, @Nullable Send<Buffer>> updater,
			UpdateReturnMode updateReturnMode,
			boolean existsAlmostCertainly) {
		return this
				.updateAndGetDelta(key, updater, existsAlmostCertainly)
				.transform(prev -> LLUtils.resolveLLDelta(prev, updateReturnMode));
	}

	default Mono<Send<Buffer>> update(Mono<Send<Buffer>> key,
			SerializationFunction<@Nullable Send<Buffer>, @Nullable Send<Buffer>> updater,
			UpdateReturnMode returnMode) {
		return update(key, updater, returnMode, false);
	}

	Mono<LLDelta> updateAndGetDelta(Mono<Send<Buffer>> key,
			SerializationFunction<@Nullable Send<Buffer>, @Nullable Send<Buffer>> updater,
			boolean existsAlmostCertainly);

	default Mono<LLDelta> updateAndGetDelta(Mono<Send<Buffer>> key,
			SerializationFunction<@Nullable Send<Buffer>, @Nullable Send<Buffer>> updater) {
		return updateAndGetDelta(key, updater, false);
	}

	Mono<Void> clear();

	Mono<Send<Buffer>> remove(Mono<Send<Buffer>> key, LLDictionaryResultType resultType);

	<K> Flux<Tuple3<K, Send<Buffer>, Optional<Send<Buffer>>>> getMulti(@Nullable LLSnapshot snapshot,
			Flux<Tuple2<K, Send<Buffer>>> keys,
			boolean existsAlmostCertainly);

	default <K> Flux<Tuple3<K, Send<Buffer>, Optional<Send<Buffer>>>> getMulti(@Nullable LLSnapshot snapshot,
			Flux<Tuple2<K, Send<Buffer>>> keys) {
		return getMulti(snapshot, keys, false);
	}

	Flux<Send<LLEntry>> putMulti(Flux<Send<LLEntry>> entries, boolean getOldValues);

	<X> Flux<ExtraKeyOperationResult<Send<Buffer>, X>> updateMulti(Flux<Tuple2<Send<Buffer>, X>> entries,
			BiSerializationFunction<Send<Buffer>, X, Send<Buffer>> updateFunction);

	Flux<Send<LLEntry>> getRange(@Nullable LLSnapshot snapshot, Mono<Send<LLRange>> range, boolean existsAlmostCertainly);

	default Flux<Send<LLEntry>> getRange(@Nullable LLSnapshot snapshot, Mono<Send<LLRange>> range) {
		return getRange(snapshot, range, false);
	}

	Flux<List<Send<LLEntry>>> getRangeGrouped(@Nullable LLSnapshot snapshot,
			Mono<Send<LLRange>> range,
			int prefixLength,
			boolean existsAlmostCertainly);

	default Flux<List<Send<LLEntry>>> getRangeGrouped(@Nullable LLSnapshot snapshot,
			Mono<Send<LLRange>> range,
			int prefixLength) {
		return getRangeGrouped(snapshot, range, prefixLength, false);
	}

	Flux<Send<Buffer>> getRangeKeys(@Nullable LLSnapshot snapshot, Mono<Send<LLRange>> range);

	Flux<List<Send<Buffer>>> getRangeKeysGrouped(@Nullable LLSnapshot snapshot, Mono<Send<LLRange>> range, int prefixLength);

	Flux<Send<Buffer>> getRangeKeyPrefixes(@Nullable LLSnapshot snapshot, Mono<Send<LLRange>> range, int prefixLength);

	Flux<BadBlock> badBlocks(Mono<Send<LLRange>> range);

	Mono<Void> setRange(Mono<Send<LLRange>> range, Flux<Send<LLEntry>> entries);

	default Mono<Void> replaceRange(Mono<Send<LLRange>> range,
			boolean canKeysChange,
			Function<Send<LLEntry>, Mono<Send<LLEntry>>> entriesReplacer,
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

	default Mono<Void> replaceRange(Mono<Send<LLRange>> range,
			boolean canKeysChange,
			Function<Send<LLEntry>, Mono<Send<LLEntry>>> entriesReplacer) {
		return replaceRange(range, canKeysChange, entriesReplacer, false);
	}

	Mono<Boolean> isRangeEmpty(@Nullable LLSnapshot snapshot, Mono<Send<LLRange>> range);

	Mono<Long> sizeRange(@Nullable LLSnapshot snapshot, Mono<Send<LLRange>> range, boolean fast);

	Mono<Send<LLEntry>> getOne(@Nullable LLSnapshot snapshot, Mono<Send<LLRange>> range);

	Mono<Send<Buffer>> getOneKey(@Nullable LLSnapshot snapshot, Mono<Send<LLRange>> range);

	Mono<Send<LLEntry>> removeOne(Mono<Send<LLRange>> range);
}
