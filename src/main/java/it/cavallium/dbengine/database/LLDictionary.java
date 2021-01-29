package it.cavallium.dbengine.database;

import java.util.Map.Entry;
import java.util.function.Function;
import org.jetbrains.annotations.Nullable;
import org.warp.commonutils.concurrency.atomicity.NotAtomic;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@NotAtomic
public interface LLDictionary extends LLKeyValueDatabaseStructure {

	Mono<byte[]> get(@Nullable LLSnapshot snapshot, byte[] key);

	Mono<byte[]> put(byte[] key, byte[] value, LLDictionaryResultType resultType);

	Mono<byte[]> remove(byte[] key, LLDictionaryResultType resultType);

	Flux<Entry<byte[], byte[]>> getMulti(@Nullable LLSnapshot snapshot, Flux<byte[]> keys);

	Flux<Entry<byte[], byte[]>> putMulti(Flux<Entry<byte[], byte[]>> entries, boolean getOldValues);

	Flux<Entry<byte[], byte[]>> getRange(@Nullable LLSnapshot snapshot, LLRange range);

	Flux<Entry<byte[], byte[]>> setRange(LLRange range, Flux<Entry<byte[], byte[]>> entries, boolean getOldValues);

	default Mono<Void> replaceRange(LLRange range, boolean canKeysChange, Function<Entry<byte[], byte[]>, Mono<Entry<byte[], byte[]>>> entriesReplacer) {
		Flux<Entry<byte[], byte[]>> replacedFlux = this.getRange(null, range).flatMap(entriesReplacer);
		if (canKeysChange) {
			return this
					.setRange(range, replacedFlux, false)
					.then();
		} else {
			return this
					.putMulti(replacedFlux, false)
					.then();
		}
	}

	Mono<Boolean> isRangeEmpty(@Nullable LLSnapshot snapshot, LLRange range);

	Mono<Long> sizeRange(@Nullable LLSnapshot snapshot, LLRange range, boolean fast);

	Mono<Entry<byte[], byte[]>> removeOne(LLRange range);
}
