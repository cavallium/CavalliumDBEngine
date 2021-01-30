package it.cavallium.dbengine.database.collections;

import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.LLDictionaryResultType;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.LLSnapshot;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

// todo: implement optimized methods
public class DatabaseMapDictionaryRange implements DatabaseStageMap<byte[], byte[], DatabaseEntry<byte[]>> {

	public static final byte[] NO_PREFIX = new byte[0];
	private final LLDictionary dictionary;
	private final byte[] keyPrefix;
	private final int keySuffixLength;
	private final LLRange range;

	private static byte[] lastKey(byte[] prefixKey, int prefixLength, int suffixLength) {
		assert prefixKey.length == prefixLength;
		byte[] lastKey = Arrays.copyOf(prefixKey, prefixLength + suffixLength);
		Arrays.fill(lastKey, prefixLength, lastKey.length, (byte) 0xFF);
		return lastKey;
	}

	private static byte[] firstKey(byte[] prefixKey, int prefixLength, int suffixLength) {
		assert prefixKey.length == prefixLength;
		byte[] lastKey = Arrays.copyOf(prefixKey, prefixLength + suffixLength);
		Arrays.fill(lastKey, prefixLength, lastKey.length, (byte) 0x00);
		return lastKey;
	}

	@SuppressWarnings("unused")
	public DatabaseMapDictionaryRange(LLDictionary dictionary, int keyLength) {
		this(dictionary, NO_PREFIX, keyLength);
	}

	public DatabaseMapDictionaryRange(LLDictionary dictionary, byte[] prefixKey, int keySuffixLength) {
		this.dictionary = dictionary;
		this.keyPrefix = prefixKey;
		this.keySuffixLength = keySuffixLength;
		byte[] firstKey = firstKey(keyPrefix, keyPrefix.length, keySuffixLength);
		byte[] lastKey = lastKey(keyPrefix, keyPrefix.length, keySuffixLength);
		this.range = keyPrefix.length == 0 ? LLRange.all() : LLRange.of(firstKey, lastKey);
	}

	private boolean suffixKeyConsistency(int keySuffixLength) {
		return this.keySuffixLength == keySuffixLength;
	}

	private byte[] toKey(byte[] suffixKey) {
		assert suffixKeyConsistency(suffixKey.length);
		byte[] key = Arrays.copyOf(keyPrefix, keyPrefix.length + suffixKey.length);
		System.arraycopy(suffixKey, 0, key, keyPrefix.length, suffixKey.length);
		return key;
	}

	private byte[] stripPrefix(byte[] key) {
		return Arrays.copyOfRange(key, this.keyPrefix.length, key.length);
	}

	private LLSnapshot resolveSnapshot(@Nullable CompositeSnapshot snapshot) {
		if (snapshot == null) {
			return null;
		} else {
			return snapshot.getSnapshot(dictionary);
		}
	}

	@Override
	public Mono<Map<byte[], byte[]>> get(@Nullable CompositeSnapshot snapshot) {
		return dictionary
				.getRange(resolveSnapshot(snapshot), range)
				.map(this::stripPrefix)
				.collectMap(Entry::getKey, Entry::getValue, HashMap::new);
	}

	@Override
	public Mono<Map<byte[], byte[]>> setAndGetPrevious(Map<byte[], byte[]> value) {
		return dictionary
				.setRange(range, Flux.fromIterable(value.entrySet()), true)
				.map(this::stripPrefix)
				.collectMap(Entry::getKey, Entry::getValue, HashMap::new);
	}

	private Entry<byte[], byte[]> stripPrefix(Entry<byte[], byte[]> entry) {
		byte[] keySuffix = stripPrefix(entry.getKey());
		return Map.entry(keySuffix, entry.getValue());
	}

	@Override
	public Mono<Map<byte[], byte[]>> clearAndGetPrevious() {
		return dictionary
				.setRange(range, Flux.empty(), true)
				.map(this::stripPrefix)
				.collectMap(Entry::getKey, Entry::getValue, HashMap::new);
	}

	@Override
	public Mono<Long> size(@Nullable CompositeSnapshot snapshot, boolean fast) {
		return dictionary.sizeRange(resolveSnapshot(snapshot), range, true);
	}

	@Override
	public Mono<DatabaseEntry<byte[]>> at(@Nullable CompositeSnapshot snapshot, byte[] keySuffix) {
		return Mono.just(new SingleDatabaseEntry(keySuffix));
	}

	@Override
	public Flux<Entry<byte[], DatabaseEntry<byte[]>>> getAllStages(@Nullable CompositeSnapshot snapshot) {
		return dictionary
				.getRangeKeys(resolveSnapshot(snapshot), range)
				.map(this::stripPrefix)
				.map(keySuffix -> Map.entry(keySuffix, new SingleDatabaseEntry(keySuffix)));
	}

	private class SingleDatabaseEntry implements DatabaseEntry<byte[]> {

		private final byte[] keySuffix;

		public SingleDatabaseEntry(byte[] keySuffix) {
			this.keySuffix = keySuffix;
		}

		@Override
		public Mono<byte[]> get(@Nullable CompositeSnapshot snapshot) {
			return dictionary.get(resolveSnapshot(snapshot), toKey(keySuffix));
		}

		@Override
		public Mono<byte[]> setAndGetPrevious(byte[] value) {
			return dictionary.put(toKey(keySuffix), value, LLDictionaryResultType.PREVIOUS_VALUE);
		}

		@Override
		public Mono<byte[]> clearAndGetPrevious() {
			return dictionary.remove(toKey(keySuffix), LLDictionaryResultType.PREVIOUS_VALUE);
		}

		@Override
		public Mono<Long> size(@Nullable CompositeSnapshot snapshot, boolean fast) {
			return dictionary
					.isRangeEmpty(resolveSnapshot(snapshot), LLRange.single(toKey(keySuffix)))
					.map(empty -> empty ? 0L : 1L);
		}

		@Override
		public Mono<Boolean> isEmpty(@Nullable CompositeSnapshot snapshot) {
			return dictionary
					.isRangeEmpty(resolveSnapshot(snapshot), LLRange.single(toKey(keySuffix)));
		}
	}
}
