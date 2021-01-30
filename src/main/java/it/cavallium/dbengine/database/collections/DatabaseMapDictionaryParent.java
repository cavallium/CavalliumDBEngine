package it.cavallium.dbengine.database.collections;

import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.LLSnapshot;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;
import reactor.core.publisher.Mono;

// todo: implement optimized methods
public abstract class DatabaseMapDictionaryParent<U, US extends DatabaseStage<U>> implements DatabaseStageMap<byte[], U, US> {

	public static final byte[] EMPTY_BYTES = new byte[0];
	private final LLDictionary dictionary;
	private final byte[] keyPrefix;
	private final int keySuffixLength;
	private final int keyExtLength;
	private final LLRange range;

	private static byte[] firstKey(byte[] prefixKey, int prefixLength, int suffixLength, int extLength) {
		return fillKeySuffixAndExt(prefixKey, prefixLength, suffixLength, extLength, (byte) 0x00);
	}

	private static byte[] lastKey(byte[] prefixKey, int prefixLength, int suffixLength, int extLength) {
		return fillKeySuffixAndExt(prefixKey, prefixLength, suffixLength, extLength, (byte) 0xFF);
	}

	private static byte[] fillKeySuffixAndExt(byte[] prefixKey, int prefixLength, int suffixLength, int extLength, byte fillValue) {
		assert prefixKey.length == prefixLength;
		assert suffixLength > 0;
		assert extLength > 0;
		byte[] result = Arrays.copyOf(prefixKey, prefixLength + suffixLength + extLength);
		Arrays.fill(result, prefixLength, result.length, fillValue);
		return result;
	}

	private static byte[] firstKey(byte[] prefixKey, byte[] suffixKey, int prefixLength, int suffixLength, int extLength) {
		return fillKeyExt(prefixKey, suffixKey, prefixLength, suffixLength, extLength, (byte) 0x00);
	}

	private static byte[] lastKey(byte[] prefixKey, byte[] suffixKey, int prefixLength, int suffixLength, int extLength) {
		return fillKeyExt(prefixKey, suffixKey, prefixLength, suffixLength, extLength, (byte) 0xFF);
	}

	private static byte[] fillKeyExt(byte[] prefixKey,
			byte[] suffixKey,
			int prefixLength,
			int suffixLength,
			int extLength,
			byte fillValue) {
		assert prefixKey.length == prefixLength;
		assert suffixKey.length == suffixLength;
		assert suffixLength > 0;
		assert extLength > 0;
		byte[] result = Arrays.copyOf(prefixKey, prefixLength + suffixLength + extLength);
		System.arraycopy(suffixKey, 0, result, prefixLength, suffixLength);
		Arrays.fill(result, prefixLength + suffixLength, result.length, fillValue);
		return result;
	}

	@SuppressWarnings("unused")
	public DatabaseMapDictionaryParent(LLDictionary dictionary, int keyLength, int keyExtLength) {
		this(dictionary, EMPTY_BYTES, keyLength, keyExtLength);
	}

	public DatabaseMapDictionaryParent(LLDictionary dictionary, byte[] prefixKey, int keySuffixLength, int keyExtLength) {
		this.dictionary = dictionary;
		this.keyPrefix = prefixKey;
		this.keySuffixLength = keySuffixLength;
		this.keyExtLength = keyExtLength;
		byte[] firstKey = firstKey(keyPrefix, keyPrefix.length, keySuffixLength, keyExtLength);
		byte[] lastKey = lastKey(keyPrefix, keyPrefix.length, keySuffixLength, keyExtLength);
		this.range = keyPrefix.length == 0 ? LLRange.all() : LLRange.of(firstKey, lastKey);
	}

	@SuppressWarnings("unused")
	private boolean suffixKeyConsistency(int keySuffixLength) {
		return this.keySuffixLength == keySuffixLength;
	}

	@SuppressWarnings("unused")
	private boolean extKeyConsistency(int keyExtLength) {
		return this.keyExtLength == keyExtLength;
	}

	@SuppressWarnings("unused")
	private boolean suffixAndExtKeyConsistency(int keySuffixAndExtLength) {
		return this.keySuffixLength + this.keyExtLength == keySuffixAndExtLength;
	}

	/**
	 * Keep only suffix and ext
	 */
	private byte[] stripPrefix(byte[] key) {
		return Arrays.copyOfRange(key, this.keyPrefix.length, key.length);
	}

	/**
	 * Remove ext from suffix
	 */
	private byte[] trimSuffix(byte[] keySuffix) {
		if (keySuffix.length == keySuffixLength) return keySuffix;
		return Arrays.copyOf(keySuffix, keySuffixLength);
	}

	/**
	 * Remove suffix from keySuffix, returning probably an empty byte array
	 */
	private byte[] stripSuffix(byte[] keySuffix) {
		if (keySuffix.length == this.keySuffixLength) return EMPTY_BYTES;
		return Arrays.copyOfRange(keySuffix, this.keySuffixLength, keySuffix.length);
	}

	private LLSnapshot resolveSnapshot(@Nullable CompositeSnapshot snapshot) {
		if (snapshot == null) {
			return null;
		} else {
			return snapshot.getSnapshot(dictionary);
		}
	}

	private LLRange toExtRange(byte[] keySuffix) {
		byte[] first = firstKey(keyPrefix, keySuffix, keyPrefix.length, keySuffixLength, keyExtLength);
		byte[] end = lastKey(keyPrefix, keySuffix, keyPrefix.length, keySuffixLength, keyExtLength);
		return LLRange.of(first, end);
	}

	@Override
	public Mono<US> at(@Nullable CompositeSnapshot snapshot, byte[] keySuffix) {
		return this.subStage(
				this.dictionary
						.getRange(resolveSnapshot(snapshot), toExtRange(stripPrefix(keySuffix)))
						.map(key -> {
							byte[] keyExt = this.stripSuffix(this.stripPrefix(key.getKey()));
							return Map.entry(keyExt, key.getValue());
						})
		);
	}

	@Override
	public Flux<Entry<byte[], US>> getAllStages(@Nullable CompositeSnapshot snapshot) {
		Flux<GroupedFlux<byte[], Entry<byte[], byte[]>>> groupedFlux = dictionary
				.getRange(resolveSnapshot(snapshot), range)
				.groupBy(entry -> this.trimSuffix(this.stripPrefix(entry.getKey())));
		return groupedFlux
				.flatMap(keyValueFlux -> this
						.subStage(keyValueFlux.map(key -> {
							byte[] keyExt = this.stripSuffix(this.stripPrefix(key.getKey()));
							return Map.entry(keyExt, key.getValue());
						}))
						.map(us -> Map.entry(keyValueFlux.key(), us))
				);
	}

	/**
	 *
	 * @param keyValueFlux a flux with keyExt and full values from the same keySuffix
	 */
	protected abstract Mono<US> subStage(Flux<Entry<byte[], byte[]>> keyValueFlux);
}
