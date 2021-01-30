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
public class DatabaseMapDictionaryParent<U, US extends DatabaseStage<U>> implements DatabaseStageMap<byte[], U, US> {

	public static final byte[] EMPTY_BYTES = new byte[0];
	private final LLDictionary dictionary;
	private final SubStageGetter<U, US> subStageGetter;
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
	public DatabaseMapDictionaryParent(LLDictionary dictionary, SubStageGetter<U, US> subStageGetter, int keyLength, int keyExtLength) {
		this(dictionary, subStageGetter, EMPTY_BYTES, keyLength, keyExtLength);
	}

	public DatabaseMapDictionaryParent(LLDictionary dictionary, SubStageGetter<U, US> subStageGetter, byte[] prefixKey, int keySuffixLength, int keyExtLength) {
		this.dictionary = dictionary;
		this.subStageGetter = subStageGetter;
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
		if (keySuffix.length == keySuffixLength)
			return keySuffix;
		return Arrays.copyOf(keySuffix, keySuffixLength);
	}

	/**
	 * Remove ext from full key
	 */
	private byte[] removeExtFromFullKey(byte[] key) {
		return Arrays.copyOf(key, keyPrefix.length + keySuffixLength);
	}

	/**
	 * Add prefix to suffix
	 */
	private byte[] toKeyWithoutExt(byte[] suffixKey) {
		assert suffixKey.length == keySuffixLength;
		byte[] result = Arrays.copyOf(keyPrefix, keyPrefix.length + keySuffixLength);
		System.arraycopy(suffixKey, 0, result, keyPrefix.length, keySuffixLength);
		return result;
	}

	/**
	 * Remove suffix from keySuffix, returning probably an empty byte array
	 */
	private byte[] stripSuffix(byte[] keySuffix) {
		if (keySuffix.length == this.keySuffixLength)
			return EMPTY_BYTES;
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
		Flux<byte[]> rangeKeys = this
				.dictionary.getRangeKeys(resolveSnapshot(snapshot), toExtRange(keySuffix)
		);
		return this.subStageGetter
				.subStage(dictionary, snapshot, toKeyWithoutExt(keySuffix), rangeKeys);
	}

	@Override
	public Flux<Entry<byte[], US>> getAllStages(@Nullable CompositeSnapshot snapshot) {
		Flux<GroupedFlux<byte[], byte[]>> groupedFlux = dictionary
				.getRangeKeys(resolveSnapshot(snapshot), range)
				.groupBy(this::removeExtFromFullKey);
		return groupedFlux
				.flatMap(rangeKeys -> this.subStageGetter
						.subStage(dictionary, snapshot, rangeKeys.key(), rangeKeys)
						.map(us -> Map.entry(rangeKeys.key(), us))
				);
	}
}
