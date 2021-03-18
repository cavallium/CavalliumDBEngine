package it.cavallium.dbengine.database;

import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Function;
import org.jetbrains.annotations.Nullable;
import org.warp.commonutils.concurrency.atomicity.NotAtomic;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@SuppressWarnings("unused")
@NotAtomic
public interface LLDictionary extends LLKeyValueDatabaseStructure {

	Mono<byte[]> get(@Nullable LLSnapshot snapshot, byte[] key, boolean existsAlmostCertainly);

	default Mono<byte[]> get(@Nullable LLSnapshot snapshot, byte[] key) {
		return get(snapshot, key, false);
	}

	Mono<byte[]> put(byte[] key, byte[] value, LLDictionaryResultType resultType);

	Mono<Boolean> update(byte[] key, Function<Optional<byte[]>, Optional<byte[]>> updater, boolean existsAlmostCertainly);

	default Mono<Boolean> update(byte[] key, Function<Optional<byte[]>, Optional<byte[]>> updater) {
		return update(key, updater, false);
	}

	Mono<Void> clear();

	Mono<byte[]> remove(byte[] key, LLDictionaryResultType resultType);

	Flux<Entry<byte[], byte[]>> getMulti(@Nullable LLSnapshot snapshot, Flux<byte[]> keys, boolean existsAlmostCertainly);

	default Flux<Entry<byte[], byte[]>> getMulti(@Nullable LLSnapshot snapshot, Flux<byte[]> keys) {
		return getMulti(snapshot, keys, false);
	}

	Flux<Entry<byte[], byte[]>> putMulti(Flux<Entry<byte[], byte[]>> entries, boolean getOldValues);

	Flux<Entry<byte[], byte[]>> getRange(@Nullable LLSnapshot snapshot, LLRange range, boolean existsAlmostCertainly);

	default Flux<Entry<byte[], byte[]>> getRange(@Nullable LLSnapshot snapshot, LLRange range) {
		return getRange(snapshot, range, false);
	}

	Flux<List<Entry<byte[], byte[]>>> getRangeGrouped(@Nullable LLSnapshot snapshot,
			LLRange range,
			int prefixLength,
			boolean existsAlmostCertainly);

	default Flux<List<Entry<byte[], byte[]>>> getRangeGrouped(@Nullable LLSnapshot snapshot,
			LLRange range,
			int prefixLength) {
		return getRangeGrouped(snapshot, range, prefixLength, false);
	}

	Flux<byte[]> getRangeKeys(@Nullable LLSnapshot snapshot, LLRange range);

	Flux<List<byte[]>> getRangeKeysGrouped(@Nullable LLSnapshot snapshot, LLRange range, int prefixLength);

	Flux<byte[]> getRangeKeyPrefixes(@Nullable LLSnapshot snapshot, LLRange range, int prefixLength);

	Flux<Entry<byte[], byte[]>> setRange(LLRange range, Flux<Entry<byte[], byte[]>> entries, boolean getOldValues);

	default Mono<Void> replaceRange(LLRange range,
			boolean canKeysChange,
			Function<Entry<byte[], byte[]>, Mono<Entry<byte[], byte[]>>> entriesReplacer,
			boolean existsAlmostCertainly) {
		return Mono.defer(() -> {
			if (canKeysChange) {
				return this
						.setRange(range, this
								.getRange(null, range, existsAlmostCertainly)
								.flatMap(entriesReplacer), false)
						.then();
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
			Function<Entry<byte[], byte[]>, Mono<Entry<byte[], byte[]>>> entriesReplacer) {
		return replaceRange(range, canKeysChange, entriesReplacer, false);
	}

	Mono<Boolean> isRangeEmpty(@Nullable LLSnapshot snapshot, LLRange range);

	Mono<Long> sizeRange(@Nullable LLSnapshot snapshot, LLRange range, boolean fast);

	Mono<Entry<byte[], byte[]>> getOne(@Nullable LLSnapshot snapshot, LLRange range);

	Mono<byte[]> getOneKey(@Nullable LLSnapshot snapshot, LLRange range);

	Mono<Entry<byte[], byte[]>> removeOne(LLRange range);
}
