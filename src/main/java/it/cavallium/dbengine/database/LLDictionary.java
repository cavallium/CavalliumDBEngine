package it.cavallium.dbengine.database;

import io.netty5.buffer.api.Buffer;
import io.netty5.buffer.api.BufferAllocator;
import io.netty5.buffer.api.Send;
import it.cavallium.dbengine.client.BadBlock;
import it.cavallium.dbengine.database.serialization.KVSerializationFunction;
import it.cavallium.dbengine.database.serialization.SerializationFunction;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@SuppressWarnings("unused")
public interface LLDictionary extends LLKeyValueDatabaseStructure {

	String getColumnName();

	BufferAllocator getAllocator();

	Mono<Send<Buffer>> get(@Nullable LLSnapshot snapshot, Mono<Send<Buffer>> key);

	Mono<Send<Buffer>> put(Mono<Send<Buffer>> key, Mono<Send<Buffer>> value, LLDictionaryResultType resultType);

	Mono<UpdateMode> getUpdateMode();

	default Mono<Send<Buffer>> update(Mono<Send<Buffer>> key,
			SerializationFunction<@Nullable Send<Buffer>, @Nullable Buffer> updater,
			UpdateReturnMode updateReturnMode) {
		return this
				.updateAndGetDelta(key, updater)
				.transform(prev -> LLUtils.resolveLLDelta(prev, updateReturnMode));
	}

	Mono<Send<LLDelta>> updateAndGetDelta(Mono<Send<Buffer>> key,
			SerializationFunction<@Nullable Send<Buffer>, @Nullable Buffer> updater);

	Mono<Void> clear();

	Mono<Send<Buffer>> remove(Mono<Send<Buffer>> key, LLDictionaryResultType resultType);

	Flux<Optional<Buffer>> getMulti(@Nullable LLSnapshot snapshot, Flux<Send<Buffer>> keys);

	Mono<Void> putMulti(Flux<Send<LLEntry>> entries);

	<K> Flux<Boolean> updateMulti(Flux<K> keys, Flux<Send<Buffer>> serializedKeys,
			KVSerializationFunction<K, @Nullable Send<Buffer>, @Nullable Buffer> updateFunction);

	Flux<Send<LLEntry>> getRange(@Nullable LLSnapshot snapshot,
			Mono<Send<LLRange>> range,
			boolean reverse,
			boolean smallRange);

	Flux<List<Send<LLEntry>>> getRangeGrouped(@Nullable LLSnapshot snapshot,
			Mono<Send<LLRange>> range,
			int prefixLength,
			boolean smallRange);

	Flux<Send<Buffer>> getRangeKeys(@Nullable LLSnapshot snapshot,
			Mono<Send<LLRange>> range,
			boolean reverse,
			boolean smallRange);

	Flux<List<Send<Buffer>>> getRangeKeysGrouped(@Nullable LLSnapshot snapshot,
			Mono<Send<LLRange>> range,
			int prefixLength,
			boolean smallRange);

	Flux<Send<Buffer>> getRangeKeyPrefixes(@Nullable LLSnapshot snapshot,
			Mono<Send<LLRange>> range,
			int prefixLength,
			boolean smallRange);

	Flux<BadBlock> badBlocks(Mono<Send<LLRange>> range);

	Mono<Void> setRange(Mono<Send<LLRange>> range, Flux<Send<LLEntry>> entries, boolean smallRange);

	default Mono<Void> replaceRange(Mono<Send<LLRange>> range,
			boolean canKeysChange,
			Function<Send<LLEntry>, Mono<Send<LLEntry>>> entriesReplacer,
			boolean smallRange) {
		return Mono.defer(() -> {
			if (canKeysChange) {
				return this
						.setRange(range, this
								.getRange(null, range, false, smallRange)
								.flatMap(entriesReplacer), smallRange);
			} else {
				return this.putMulti(this.getRange(null, range, false, smallRange).flatMap(entriesReplacer));
			}
		});
	}

	Mono<Boolean> isRangeEmpty(@Nullable LLSnapshot snapshot, Mono<Send<LLRange>> range, boolean fillCache);

	Mono<Long> sizeRange(@Nullable LLSnapshot snapshot, Mono<Send<LLRange>> range, boolean fast);

	Mono<Send<LLEntry>> getOne(@Nullable LLSnapshot snapshot, Mono<Send<LLRange>> range);

	Mono<Send<Buffer>> getOneKey(@Nullable LLSnapshot snapshot, Mono<Send<LLRange>> range);

	Mono<Send<LLEntry>> removeOne(Mono<Send<LLRange>> range);
}
