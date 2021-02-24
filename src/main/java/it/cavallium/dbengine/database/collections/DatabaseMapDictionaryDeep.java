package it.cavallium.dbengine.database.collections;

import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.serialization.SerializerFixedBinaryLength;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuples;

// todo: implement optimized methods
public class DatabaseMapDictionaryDeep<T, U, US extends DatabaseStage<U>> implements DatabaseStageMap<T, U, US> {

	public static final byte[] EMPTY_BYTES = new byte[0];
	protected final LLDictionary dictionary;
	protected final SubStageGetter<U, US> subStageGetter;
	protected final SerializerFixedBinaryLength<T, byte[]> keySuffixSerializer;
	protected final byte[] keyPrefix;
	protected final int keySuffixLength;
	protected final int keyExtLength;
	protected final LLRange range;

	protected static byte[] firstKey(byte[] prefixKey, int prefixLength, int suffixLength, int extLength) {
		return fillKeySuffixAndExt(prefixKey, prefixLength, suffixLength, extLength, (byte) 0x00);
	}

	protected static byte[] lastKey(byte[] prefixKey, int prefixLength, int suffixLength, int extLength) {
		return fillKeySuffixAndExt(prefixKey, prefixLength, suffixLength, extLength, (byte) 0xFF);
	}

	protected static byte[] fillKeySuffixAndExt(byte[] prefixKey,
			int prefixLength,
			int suffixLength,
			int extLength,
			byte fillValue) {
		assert prefixKey.length == prefixLength;
		assert suffixLength > 0;
		assert extLength >= 0;
		byte[] result = Arrays.copyOf(prefixKey, prefixLength + suffixLength + extLength);
		Arrays.fill(result, prefixLength, result.length, fillValue);
		return result;
	}

	protected static byte[] firstKey(byte[] prefixKey,
			byte[] suffixKey,
			int prefixLength,
			int suffixLength,
			int extLength) {
		return fillKeyExt(prefixKey, suffixKey, prefixLength, suffixLength, extLength, (byte) 0x00);
	}

	protected static byte[] lastKey(byte[] prefixKey,
			byte[] suffixKey,
			int prefixLength,
			int suffixLength,
			int extLength) {
		return fillKeyExt(prefixKey, suffixKey, prefixLength, suffixLength, extLength, (byte) 0xFF);
	}

	protected static byte[] fillKeyExt(byte[] prefixKey,
			byte[] suffixKey,
			int prefixLength,
			int suffixLength,
			int extLength,
			byte fillValue) {
		assert prefixKey.length == prefixLength;
		assert suffixKey.length == suffixLength;
		assert suffixLength > 0;
		assert extLength >= 0;
		byte[] result = Arrays.copyOf(prefixKey, prefixLength + suffixLength + extLength);
		System.arraycopy(suffixKey, 0, result, prefixLength, suffixLength);
		Arrays.fill(result, prefixLength + suffixLength, result.length, fillValue);
		return result;
	}

	/**
	 * Use DatabaseMapDictionaryRange.simple instead
	 */
	@Deprecated
	public static <T, U> DatabaseMapDictionaryDeep<T, U, DatabaseStageEntry<U>> simple(LLDictionary dictionary,
			SerializerFixedBinaryLength<T, byte[]> keySerializer,
			SubStageGetterSingle<U> subStageGetter) {
		return new DatabaseMapDictionaryDeep<>(dictionary, EMPTY_BYTES, keySerializer, subStageGetter, 0);
	}

	public static <T, U, US extends DatabaseStage<U>> DatabaseMapDictionaryDeep<T, U, US> deepTail(LLDictionary dictionary,
			SerializerFixedBinaryLength<T, byte[]> keySerializer,
			int keyExtLength,
			SubStageGetter<U, US> subStageGetter) {
		return new DatabaseMapDictionaryDeep<>(dictionary, EMPTY_BYTES, keySerializer, subStageGetter, keyExtLength);
	}

	public static <T, U, US extends DatabaseStage<U>> DatabaseMapDictionaryDeep<T, U, US> deepIntermediate(LLDictionary dictionary,
			byte[] prefixKey,
			SerializerFixedBinaryLength<T, byte[]> keySuffixSerializer,
			SubStageGetter<U, US> subStageGetter,
			int keyExtLength) {
		return new DatabaseMapDictionaryDeep<>(dictionary, prefixKey, keySuffixSerializer, subStageGetter, keyExtLength);
	}

	protected DatabaseMapDictionaryDeep(LLDictionary dictionary,
			byte[] prefixKey,
			SerializerFixedBinaryLength<T, byte[]> keySuffixSerializer,
			SubStageGetter<U, US> subStageGetter,
			int keyExtLength) {
		this.dictionary = dictionary;
		this.subStageGetter = subStageGetter;
		this.keySuffixSerializer = keySuffixSerializer;
		this.keyPrefix = prefixKey;
		this.keySuffixLength = keySuffixSerializer.getSerializedBinaryLength();
		this.keyExtLength = keyExtLength;
		byte[] firstKey = firstKey(keyPrefix, keyPrefix.length, keySuffixLength, keyExtLength);
		byte[] lastKey = lastKey(keyPrefix, keyPrefix.length, keySuffixLength, keyExtLength);
		this.range = keyPrefix.length == 0 ? LLRange.all() : LLRange.of(firstKey, lastKey);
		assert subStageKeysConsistency(keyPrefix.length + keySuffixLength + keyExtLength);
	}

	@SuppressWarnings("unused")
	protected boolean suffixKeyConsistency(int keySuffixLength) {
		return this.keySuffixLength == keySuffixLength;
	}

	@SuppressWarnings("unused")
	protected boolean extKeyConsistency(int keyExtLength) {
		return this.keyExtLength == keyExtLength;
	}

	@SuppressWarnings("unused")
	protected boolean suffixAndExtKeyConsistency(int keySuffixAndExtLength) {
		return this.keySuffixLength + this.keyExtLength == keySuffixAndExtLength;
	}

	/**
	 * Keep only suffix and ext
	 */
	protected byte[] stripPrefix(byte[] key) {
		return Arrays.copyOfRange(key, this.keyPrefix.length, key.length);
	}

	/**
	 * Remove ext from full key
	 */
	protected byte[] removeExtFromFullKey(byte[] key) {
		return Arrays.copyOf(key, keyPrefix.length + keySuffixLength);
	}

	/**
	 * Add prefix to suffix
	 */
	protected byte[] toKeyWithoutExt(byte[] suffixKey) {
		assert suffixKey.length == keySuffixLength;
		byte[] result = Arrays.copyOf(keyPrefix, keyPrefix.length + keySuffixLength);
		System.arraycopy(suffixKey, 0, result, keyPrefix.length, keySuffixLength);
		assert result.length == keyPrefix.length + keySuffixLength;
		return result;
	}

	protected LLSnapshot resolveSnapshot(@Nullable CompositeSnapshot snapshot) {
		if (snapshot == null) {
			return null;
		} else {
			return snapshot.getSnapshot(dictionary);
		}
	}

	protected LLRange toExtRange(byte[] keySuffix) {
		byte[] first = firstKey(keyPrefix, keySuffix, keyPrefix.length, keySuffixLength, keyExtLength);
		byte[] end = lastKey(keyPrefix, keySuffix, keyPrefix.length, keySuffixLength, keyExtLength);
		return LLRange.of(first, end);
	}

	@Override
	public Mono<Long> leavesCount(@Nullable CompositeSnapshot snapshot, boolean fast) {
		return dictionary.sizeRange(resolveSnapshot(snapshot), range, fast);
	}

	@SuppressWarnings("ReactiveStreamsUnusedPublisher")
	@Override
	public Mono<US> at(@Nullable CompositeSnapshot snapshot, T keySuffix) {
		byte[] keySuffixData = serializeSuffix(keySuffix);
		return this.subStageGetter
				.subStage(dictionary,
						snapshot,
						toKeyWithoutExt(keySuffixData),
						this.subStageGetter.needsKeyFlux()
								? this.dictionary.getRangeKeys(resolveSnapshot(snapshot), toExtRange(keySuffixData))
								: Flux.empty()
				);
	}

	@Override
	public Flux<Entry<T, US>> getAllStages(@Nullable CompositeSnapshot snapshot) {
		return Flux.defer(() -> {
			if (this.subStageGetter.needsKeyFlux()) {
				return dictionary
						.getRangeKeysGrouped(resolveSnapshot(snapshot), range, keyPrefix.length + keySuffixLength)
						.flatMap(rangeKeys -> {
							byte[] groupKeyWithExt = rangeKeys.get(0);
							byte[] groupKeyWithoutExt = removeExtFromFullKey(groupKeyWithExt);
							byte[] groupSuffix = this.stripPrefix(groupKeyWithoutExt);
							assert subStageKeysConsistency(groupKeyWithExt.length);
							return this.subStageGetter
									.subStage(dictionary,
											snapshot,
											groupKeyWithoutExt,
											this.subStageGetter.needsKeyFlux() ? Flux.defer(() -> Flux.fromIterable(rangeKeys)) : Flux.empty()
									)
									.map(us -> Map.entry(this.deserializeSuffix(groupSuffix), us));
						});
			} else {
				return dictionary
						.getOneKey(resolveSnapshot(snapshot), range)
						.flatMap(randomKeyWithExt -> {
							byte[] keyWithoutExt = removeExtFromFullKey(randomKeyWithExt);
							byte[] keySuffix = this.stripPrefix(keyWithoutExt);
							assert subStageKeysConsistency(keyWithoutExt.length);
							return this.subStageGetter
									.subStage(dictionary, snapshot, keyWithoutExt, Mono.just(randomKeyWithExt).flux())
									.map(us -> Map.entry(this.deserializeSuffix(keySuffix), us));
						});
			}
		});

	}

	private boolean subStageKeysConsistency(int totalKeyLength) {
		if (subStageGetter instanceof SubStageGetterMapDeep) {
			return totalKeyLength
					== keyPrefix.length + keySuffixLength + ((SubStageGetterMapDeep<?, ?, ?>) subStageGetter).getKeyBinaryLength();
		} else if (subStageGetter instanceof SubStageGetterMap) {
			return totalKeyLength
					== keyPrefix.length + keySuffixLength + ((SubStageGetterMap<?, ?>) subStageGetter).getKeyBinaryLength();
		} else {
			return true;
		}
	}

	@Override
	public Flux<Entry<T, U>> setAllValuesAndGetPrevious(Flux<Entry<T, U>> entries) {
		return getAllStages(null)
				.flatMap(stage -> stage.getValue().get(null).map(val -> Map.entry(stage.getKey(), val)))
				.concatWith(entries
						.flatMap(entry -> at(null, entry.getKey()).map(us -> Tuples.of(us, entry.getValue())))
						.flatMap(tuple -> tuple.getT1().set(tuple.getT2()))
						.then(Mono.empty()));
	}

	//todo: temporary wrapper. convert the whole class to buffers
	protected T deserializeSuffix(byte[] keySuffix) {
		assert suffixKeyConsistency(keySuffix.length);
		return keySuffixSerializer.deserialize(keySuffix);
	}

	//todo: temporary wrapper. convert the whole class to buffers
	protected byte[] serializeSuffix(T keySuffix) {
		byte[] suffixData = keySuffixSerializer.serialize(keySuffix);
		assert suffixKeyConsistency(suffixData.length);
		return suffixData;
	}
}
