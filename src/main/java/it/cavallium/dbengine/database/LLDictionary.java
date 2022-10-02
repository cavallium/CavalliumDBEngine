package it.cavallium.dbengine.database;

import io.netty5.buffer.Buffer;
import io.netty5.buffer.BufferAllocator;
import io.netty5.util.Send;
import it.cavallium.dbengine.client.BadBlock;
import it.cavallium.dbengine.database.disk.BinarySerializationFunction;
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

	Mono<Buffer> get(@Nullable LLSnapshot snapshot, Mono<Buffer> key);

	Mono<Buffer> put(Mono<Buffer> key, Mono<Buffer> value, LLDictionaryResultType resultType);

	UpdateMode getUpdateMode();

	default Mono<Buffer> update(Mono<Buffer> key,
			BinarySerializationFunction updater,
			UpdateReturnMode updateReturnMode) {
		return this
				.updateAndGetDelta(key, updater)
				.transform(prev -> LLUtils.resolveLLDelta(prev, updateReturnMode));
	}

	Mono<LLDelta> updateAndGetDelta(Mono<Buffer> key, BinarySerializationFunction updater);

	Mono<Void> clear();

	Mono<Buffer> remove(Mono<Buffer> key, LLDictionaryResultType resultType);

	Flux<OptionalBuf> getMulti(@Nullable LLSnapshot snapshot, Flux<Buffer> keys);

	Mono<Void> putMulti(Flux<LLEntry> entries);

	<K> Flux<Boolean> updateMulti(Flux<K> keys, Flux<Buffer> serializedKeys,
			KVSerializationFunction<K, @Nullable Buffer, @Nullable Buffer> updateFunction);

	Flux<LLEntry> getRange(@Nullable LLSnapshot snapshot,
			Mono<LLRange> range,
			boolean reverse,
			boolean smallRange);

	Flux<List<LLEntry>> getRangeGrouped(@Nullable LLSnapshot snapshot,
			Mono<LLRange> range,
			int prefixLength,
			boolean smallRange);

	Flux<Buffer> getRangeKeys(@Nullable LLSnapshot snapshot,
			Mono<LLRange> range,
			boolean reverse,
			boolean smallRange);

	Flux<List<Buffer>> getRangeKeysGrouped(@Nullable LLSnapshot snapshot,
			Mono<LLRange> range,
			int prefixLength,
			boolean smallRange);

	Flux<Buffer> getRangeKeyPrefixes(@Nullable LLSnapshot snapshot,
			Mono<LLRange> range,
			int prefixLength,
			boolean smallRange);

	Flux<BadBlock> badBlocks(Mono<LLRange> range);

	Mono<Void> setRange(Mono<LLRange> range, Flux<LLEntry> entries, boolean smallRange);

	default Mono<Void> replaceRange(Mono<LLRange> range,
			boolean canKeysChange,
			Function<LLEntry, Mono<LLEntry>> entriesReplacer,
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

	Mono<Boolean> isRangeEmpty(@Nullable LLSnapshot snapshot, Mono<LLRange> range, boolean fillCache);

	Mono<Long> sizeRange(@Nullable LLSnapshot snapshot, Mono<LLRange> range, boolean fast);

	Mono<LLEntry> getOne(@Nullable LLSnapshot snapshot, Mono<LLRange> range);

	Mono<Buffer> getOneKey(@Nullable LLSnapshot snapshot, Mono<LLRange> range);

	Mono<LLEntry> removeOne(Mono<LLRange> range);
}
