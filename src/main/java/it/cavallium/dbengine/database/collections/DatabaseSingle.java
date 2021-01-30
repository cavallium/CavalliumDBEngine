package it.cavallium.dbengine.database.collections;

import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.LLDictionaryResultType;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.LLSnapshot;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Mono;

public class DatabaseSingle implements DatabaseStageEntry<byte[]> {

	private final LLDictionary dictionary;
	private final byte[] key;

	public DatabaseSingle(LLDictionary dictionary, byte[] key) {
		this.dictionary = dictionary;
		this.key = key;
	}

	private LLSnapshot resolveSnapshot(@Nullable CompositeSnapshot snapshot) {
		if (snapshot == null) {
			return null;
		} else {
			return snapshot.getSnapshot(dictionary);
		}
	}

	@Override
	public Mono<byte[]> get(@Nullable CompositeSnapshot snapshot) {
		return dictionary.get(resolveSnapshot(snapshot), key);
	}

	@Override
	public Mono<byte[]> setAndGetPrevious(byte[] value) {
		return dictionary.put(key, value, LLDictionaryResultType.PREVIOUS_VALUE);
	}

	@Override
	public Mono<byte[]> clearAndGetPrevious() {
		return dictionary.remove(key, LLDictionaryResultType.PREVIOUS_VALUE);
	}

	@Override
	public Mono<Long> size(@Nullable CompositeSnapshot snapshot, boolean fast) {
		return dictionary
				.isRangeEmpty(resolveSnapshot(snapshot), LLRange.single(key))
				.map(empty -> empty ? 0L : 1L);
	}

	@Override
	public Mono<Boolean> isEmpty(@Nullable CompositeSnapshot snapshot) {
		return dictionary
				.isRangeEmpty(resolveSnapshot(snapshot), LLRange.single(key));
	}
}