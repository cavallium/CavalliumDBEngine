package it.cavallium.dbengine.database.collections;

import static it.cavallium.dbengine.utils.StreamUtils.resourceStream;

import it.cavallium.buffer.Buf;
import it.cavallium.buffer.BufDataInput;
import it.cavallium.buffer.BufDataOutput;
import it.cavallium.dbengine.client.DbProgress;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.client.SSTVerificationProgress;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.LLDictionaryResultType;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.SubStageEntry;
import it.cavallium.dbengine.database.UpdateMode;
import it.cavallium.dbengine.database.collections.DatabaseEmpty.Nothing;
import it.cavallium.dbengine.database.serialization.SerializationException;
import it.cavallium.dbengine.database.serialization.Serializer;
import it.cavallium.dbengine.database.serialization.SerializerFixedBinaryLength;
import it.unimi.dsi.fastutil.objects.Object2ObjectSortedMap;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import org.apache.commons.lang3.function.TriFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

// todo: implement optimized methods (which?)
public class DatabaseMapDictionaryDeep<T, U, US extends DatabaseStage<U>> implements DatabaseStageMap<T, U, US> {

	private static final Logger LOG = LogManager.getLogger(DatabaseMapDictionaryDeep.class);

	protected final LLDictionary dictionary;
	private final AtomicLong totalZeroBytesErrors = new AtomicLong();
	protected final SubStageGetter<U, US> subStageGetter;
	protected final SerializerFixedBinaryLength<T> keySuffixSerializer;
	protected final int keyPrefixLength;
	protected final int keySuffixLength;
	protected final int keyExtLength;
	protected final LLRange range;

	protected Buf keyPrefix;

	private static void incrementPrefix(Buf modifiablePrefix, int prefixLength) {
		assert modifiablePrefix.size() >= prefixLength;
		final var originalKeyLength = modifiablePrefix.size();
		boolean overflowed = true;
		final int ff = 0xFF;
		int writtenBytes = 0;
		for (int i = prefixLength - 1; i >= 0; i--) {
			int iByte = Byte.toUnsignedInt(modifiablePrefix.getByte(i));
			if (iByte != ff) {
				modifiablePrefix.set(i, (byte) (iByte + 1));
				writtenBytes++;
				overflowed = false;
				break;
			} else {
				modifiablePrefix.set(i, (byte) 0x00);
				writtenBytes++;
			}
		}
		assert prefixLength - writtenBytes >= 0;

		if (overflowed) {
			modifiablePrefix.add((byte) 0);
			for (int i = 0; i < originalKeyLength; i++) {
				modifiablePrefix.set(i, (byte) 0xFF);
			}
			modifiablePrefix.set(originalKeyLength, (byte) 0x00);
		}
	}

	@VisibleForTesting
	public static Buf firstRangeKey(Buf prefixKey, int prefixLength, Buf suffixAndExtZeroes) {
		return createFullKeyWithEmptySuffixAndExt(prefixKey, prefixLength, suffixAndExtZeroes);
	}

	@VisibleForTesting
	public static Buf nextRangeKey(Buf prefixKey, int prefixLength, Buf suffixAndExtZeroes) {
		Buf modifiablePrefixKey = createFullKeyWithEmptySuffixAndExt(prefixKey, prefixLength, suffixAndExtZeroes);
		incrementPrefix(modifiablePrefixKey, prefixLength);
		return modifiablePrefixKey;
	}

	private static Buf createFullKeyWithEmptySuffixAndExt(Buf prefixKey, int prefixLength, Buf suffixAndExtZeroes) {
		var modifiablePrefixKey = Buf.create(prefixLength + suffixAndExtZeroes.size());
		if (prefixKey != null) {
			modifiablePrefixKey.addAll(prefixKey);
		}
		assert prefixKey != null || prefixLength == 0 : "Prefix length is " +  prefixLength + " but the prefix key is null";
		zeroFillKeySuffixAndExt(modifiablePrefixKey, prefixLength, suffixAndExtZeroes);
		return modifiablePrefixKey;
	}

	/**
	 * @param modifiablePrefixKey This field content will be modified
	 */
	protected static void zeroFillKeySuffixAndExt(@NotNull Buf modifiablePrefixKey, int prefixLength, Buf suffixAndExtZeroes) {
		//noinspection UnnecessaryLocalVariable
		var result = modifiablePrefixKey;
		var suffixLengthAndExtLength = suffixAndExtZeroes.size();
		assert result.size() == prefixLength;
		assert suffixLengthAndExtLength > 0 : "Suffix length + ext length is < 0: " + suffixLengthAndExtLength;
		result.size(prefixLength);
		modifiablePrefixKey.addAll(suffixAndExtZeroes);
		assert modifiablePrefixKey.size() == prefixLength + suffixAndExtZeroes.size() : "Result buffer size is wrong";
	}

	/**
	 * Use DatabaseMapDictionaryRange.simple instead
	 */
	@Deprecated
	public static <T, U> DatabaseMapDictionaryDeep<T, U, DatabaseStageEntry<U>> simple(LLDictionary dictionary,
			SerializerFixedBinaryLength<T> keySerializer, SubStageGetterSingle<U> subStageGetter) {
		return new DatabaseMapDictionaryDeep<>(dictionary, null, keySerializer, subStageGetter, 0);
	}

	public static <T, U, US extends DatabaseStage<U>> DatabaseMapDictionaryDeep<T, U, US> deepTail(
			LLDictionary dictionary, SerializerFixedBinaryLength<T> keySerializer, int keyExtLength,
			SubStageGetter<U, US> subStageGetter) {
		return new DatabaseMapDictionaryDeep<>(dictionary, null, keySerializer, subStageGetter, keyExtLength);
	}

	public static <T, U, US extends DatabaseStage<U>> DatabaseMapDictionaryDeep<T, U, US> deepIntermediate(
			LLDictionary dictionary, Buf prefixKey, SerializerFixedBinaryLength<T> keySuffixSerializer,
			SubStageGetter<U, US> subStageGetter, int keyExtLength) {
		return new DatabaseMapDictionaryDeep<>(dictionary, prefixKey, keySuffixSerializer, subStageGetter, keyExtLength);
	}

	protected DatabaseMapDictionaryDeep(LLDictionary dictionary, @Nullable Buf prefixKey,
			SerializerFixedBinaryLength<T> keySuffixSerializer, SubStageGetter<U, US> subStageGetter, int keyExtLength) {
		this.dictionary = dictionary;
		this.subStageGetter = subStageGetter;
		this.keySuffixSerializer = keySuffixSerializer;
		this.keyPrefixLength = prefixKey != null ? prefixKey.size() : 0;
		this.keySuffixLength = keySuffixSerializer.getSerializedBinaryLength();
		this.keyExtLength = keyExtLength;
		var keySuffixAndExtZeroBuffer = Buf.createZeroes(keySuffixLength + keyExtLength);
		assert keySuffixAndExtZeroBuffer.size() == keySuffixLength + keyExtLength :
				"Key suffix and ext zero buffer readable length is not equal"
						+ " to the key suffix length + key ext length. keySuffixAndExtZeroBuffer="
						+ keySuffixAndExtZeroBuffer.size() + " keySuffixLength=" + keySuffixLength + " keyExtLength="
						+ keyExtLength;
		assert keySuffixAndExtZeroBuffer.size() > 0;
		var firstKey = firstRangeKey(prefixKey, keyPrefixLength, keySuffixAndExtZeroBuffer);
		var nextRangeKey = nextRangeKey(prefixKey, keyPrefixLength, keySuffixAndExtZeroBuffer);
		assert keyPrefixLength == 0 || !LLUtils.equals(firstKey, nextRangeKey);
		if (keyPrefixLength == 0) {
			this.range = LLRange.all();
		} else {
			this.range = LLRange.of(firstKey, nextRangeKey);
		}
		assert subStageKeysConsistency(keyPrefixLength + keySuffixLength + keyExtLength);

		this.keyPrefix = prefixKey;
	}
	private DatabaseMapDictionaryDeep(LLDictionary dictionary,
			SubStageGetter<U, US> subStageGetter,
			SerializerFixedBinaryLength<T> keySuffixSerializer,
			int keyPrefixLength,
			int keySuffixLength,
			int keyExtLength,
			LLRange range,
			Buf keyPrefix) {
		this.dictionary = dictionary;
		this.subStageGetter = subStageGetter;
		this.keySuffixSerializer = keySuffixSerializer;
		this.keyPrefixLength = keyPrefixLength;
		this.keySuffixLength = keySuffixLength;
		this.keyExtLength = keyExtLength;
		this.range = range;

		this.keyPrefix = keyPrefix;
	}

	@SuppressWarnings("unused")
	protected boolean suffixKeyLengthConsistency(int keySuffixLength) {
		assert
				this.keySuffixLength == keySuffixLength :
				"Key suffix length is " + keySuffixLength + ", but it should be " + this.keySuffixLength + " bytes long";
		//noinspection ConstantValue
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
	 * @return the prefix
	 */
	protected Buf prefixSubList(Buf key) {
		assert key.size() == keyPrefixLength + keySuffixLength + keyExtLength
				|| key.size() == keyPrefixLength + keySuffixLength;
		return key.subList(0, this.keyPrefixLength);
	}

	/**
	 * @return the suffix
	 */
	protected Buf suffixSubList(Buf key) {
		assert key.size() == keyPrefixLength + keySuffixLength + keyExtLength
				|| key.size() == keyPrefixLength + keySuffixLength;
		return key.subList(this.keyPrefixLength, keyPrefixLength + keySuffixLength);
	}

	/**
	 * @return the suffix
	 */
	protected Buf suffixAndExtSubList(Buf key) {
		assert key.size() == keyPrefixLength + keySuffixLength + keyExtLength
				|| key.size() == keyPrefixLength + keySuffixLength;
		return key.subList(this.keyPrefixLength, key.size());
	}

	/**
	 * @return the ext
	 */
	protected Buf extSubList(Buf key) {
		assert key.size() == keyPrefixLength + keySuffixLength + keyExtLength
				|| key.size() == keyPrefixLength + keySuffixLength;
		return key.subList(this.keyPrefixLength + this.keySuffixLength, key.size());
	}

	protected LLSnapshot resolveSnapshot(@Nullable CompositeSnapshot snapshot) {
		if (snapshot == null) {
			return null;
		} else {
			return snapshot.getSnapshot(dictionary);
		}
	}

	@Override
	public long leavesCount(@Nullable CompositeSnapshot snapshot, boolean fast) {
		return dictionary.sizeRange(resolveSnapshot(snapshot), range, fast);
	}

	@Override
	public boolean isEmpty(@Nullable CompositeSnapshot snapshot) {
		return dictionary.isRangeEmpty(resolveSnapshot(snapshot), range, false);
	}

	@Override
	public @NotNull US at(@Nullable CompositeSnapshot snapshot, T keySuffix) {
		BufDataOutput bufOutput = BufDataOutput.createLimited(keyPrefixLength + keySuffixLength + keyExtLength);
		if (keyPrefix != null) {
			bufOutput.writeBytes(keyPrefix);
		}
		serializeSuffixTo(keySuffix, bufOutput);
		return this.subStageGetter.subStage(dictionary, snapshot, bufOutput.asList());
	}

	@Override
	public UpdateMode getUpdateMode() {
		return dictionary.getUpdateMode();
	}

	@Override
	public Stream<DbProgress<SSTVerificationProgress>> verifyChecksum() {
		return dictionary.verifyChecksum(range);
	}

	@Override
	public Stream<SubStageEntry<T, US>> getAllStages(@Nullable CompositeSnapshot snapshot, boolean smallRange) {
		return dictionary
				.getRangeKeyPrefixes(resolveSnapshot(snapshot), range, keyPrefixLength + keySuffixLength, smallRange)
				.map(groupKeyWithoutExt -> {
					T deserializedSuffix;
					var splittedGroupSuffix = suffixSubList(groupKeyWithoutExt);
					deserializedSuffix = this.deserializeSuffix(BufDataInput.create(splittedGroupSuffix));
					return new SubStageEntry<>(deserializedSuffix,
							this.subStageGetter.subStage(dictionary, snapshot, groupKeyWithoutExt));
				});
	}

	private boolean subStageKeysConsistency(int totalKeyLength) {
		if (subStageGetter instanceof SubStageGetterMapDeep) {
			return totalKeyLength
					== keyPrefixLength + keySuffixLength + ((SubStageGetterMapDeep<?, ?, ?>) subStageGetter).getKeyBinaryLength();
		} else if (subStageGetter instanceof SubStageGetterMap) {
			return totalKeyLength
					== keyPrefixLength + keySuffixLength + ((SubStageGetterMap<?, ?>) subStageGetter).getKeyBinaryLength();
		} else {
			return true;
		}
	}

	@Override
	public void setAllEntries(Stream<Entry<T, U>> entries) {
		this.clear();
		this.putMulti(entries);
	}

	@Override
	public Stream<Entry<T, U>> setAllEntriesAndGetPrevious(Stream<Entry<T, U>> entries) {
		return resourceStream(() -> this.getAllEntries(null, false), () -> setAllEntries(entries));
	}

	@Override
	public void clear() {
		if (range.isAll()) {
			dictionary.clear();
		} else if (range.isSingle()) {
			dictionary.remove(range.getSingleUnsafe(), LLDictionaryResultType.VOID);
		} else {
			dictionary.setRange(range, Stream.empty(), false);
		}
	}

	protected T deserializeSuffix(@NotNull BufDataInput keySuffix) throws SerializationException {
		assert suffixKeyLengthConsistency(keySuffix.available());
		return keySuffixSerializer.deserialize(keySuffix);
	}

	protected void serializeSuffixTo(T keySuffix, BufDataOutput output) throws SerializationException {
		var beforeWriterOffset = output.size();
		assert beforeWriterOffset == keyPrefixLength;
		assert keySuffixSerializer.getSerializedBinaryLength() == keySuffixLength
				: "Invalid key suffix serializer length: " +  keySuffixSerializer.getSerializedBinaryLength()
				+ ". Expected: " + keySuffixLength;
		keySuffixSerializer.serialize(keySuffix, output);
		var afterWriterOffset = output.size();
		assert suffixKeyLengthConsistency(afterWriterOffset - beforeWriterOffset)
				: "Invalid key suffix length: " + (afterWriterOffset - beforeWriterOffset) + ". Expected: " + keySuffixLength;
	}

	public static <K1, K2, V, R> Stream<R> getAllLeaves2(DatabaseMapDictionaryDeep<K1, Object2ObjectSortedMap<K2, V>, ? extends DatabaseStageMap<K2, V, DatabaseStageEntry<V>>> deepMap,
			CompositeSnapshot snapshot,
			TriFunction<K1, K2, V, R> merger,
			@Nullable K1 savedProgressKey1) {
		var keySuffix1Serializer = deepMap.keySuffixSerializer;
		SerializerFixedBinaryLength<?> keySuffix2Serializer;
		Serializer<?> valueSerializer;
		boolean isHashed;
		boolean isHashedSet;
		if (deepMap.subStageGetter instanceof SubStageGetterMap subStageGetterMap) {
			isHashed = false;
			isHashedSet = false;
			keySuffix2Serializer = subStageGetterMap.keySerializer;
			valueSerializer = subStageGetterMap.valueSerializer;
		} else if (deepMap.subStageGetter instanceof SubStageGetterHashMap subStageGetterHashMap) {
			isHashed = true;
			isHashedSet = false;
			keySuffix2Serializer = subStageGetterHashMap.keyHashSerializer;

			//noinspection unchecked
			ValueWithHashSerializer<K2, V> valueWithHashSerializer = new ValueWithHashSerializer<>(
					(Serializer<K2>) subStageGetterHashMap.keySerializer,
					(Serializer<V>) subStageGetterHashMap.valueSerializer
			);
			valueSerializer = new ValuesSetSerializer<>(valueWithHashSerializer);
		} else if (deepMap.subStageGetter instanceof SubStageGetterHashSet subStageGetterHashSet) {
			isHashed = true;
			isHashedSet = true;
			keySuffix2Serializer = subStageGetterHashSet.keyHashSerializer;

			//noinspection unchecked
			valueSerializer = new ValuesSetSerializer<K2>(subStageGetterHashSet.keySerializer);
		} else {
			throw new IllegalArgumentException();
		}

		var firstKey = Optional.ofNullable(savedProgressKey1);
		var fullRange = deepMap.range;


		LLRange range;
		if (firstKey.isPresent()) {
			var key1Buf = BufDataOutput.create(keySuffix1Serializer.getSerializedBinaryLength());
			keySuffix1Serializer.serialize(firstKey.get(), key1Buf);
			range = LLRange.of(key1Buf.asList(), fullRange.getMax());
		} else {
			range = fullRange;
		}

		return deepMap.dictionary.getRange(deepMap.resolveSnapshot(snapshot), range, false, false)
				.flatMap(entry -> {
					K1 key1 = null;
					Object key2 = null;
					try {
						var keyBuf = entry.getKey();
						var valueBuf = entry.getValue();
						try {
							assert keyBuf != null;
							var suffix1And2 = BufDataInput.create(keyBuf.subList(deepMap.keyPrefixLength, deepMap.keyPrefixLength + deepMap.keySuffixLength + deepMap.keyExtLength));
							key1 = keySuffix1Serializer.deserialize(suffix1And2);
							key2 = keySuffix2Serializer.deserialize(suffix1And2);
							assert valueBuf != null;
							Object value = valueSerializer.deserialize(BufDataInput.create(valueBuf));
							if (isHashedSet) {
								//noinspection unchecked
								Set<K2> set = (Set<K2>) value;
								K1 finalKey1 = key1;
								//noinspection unchecked
								return set.stream().map(e -> merger.apply(finalKey1, e, (V) Nothing.INSTANCE));
							} else if (isHashed) {
								//noinspection unchecked
								Set<Entry<K2, V>> set = (Set<Entry<K2, V>>) value;
								K1 finalKey1 = key1;
								return set.stream().map(e -> merger.apply(finalKey1, e.getKey(), e.getValue()));
							} else {
								//noinspection unchecked
								return Stream.of(merger.apply(key1, (K2) key2, (V) value));
							}
						} catch (IndexOutOfBoundsException ex) {
							var exMessage = ex.getMessage();
							if (exMessage != null && exMessage.contains("read 0 to 0, write 0 to ")) {
								var totalZeroBytesErrors = deepMap.totalZeroBytesErrors.incrementAndGet();
								if (totalZeroBytesErrors < 512 || totalZeroBytesErrors % 10000 == 0) {
									LOG.error("Unexpected zero-bytes value at " + deepMap.dictionary.getDatabaseName()
											+ ":" + deepMap.dictionary.getColumnName()
											+ ":[" + key1
											+ ":" + key2
											+ "](" + LLUtils.toStringSafe(keyBuf) + ") total=" + totalZeroBytesErrors);
								}
								return Stream.empty();
							} else {
								throw ex;
							}
						}
					} catch (SerializationException ex) {
						throw new CompletionException(ex);
					}
				});
	}
}
