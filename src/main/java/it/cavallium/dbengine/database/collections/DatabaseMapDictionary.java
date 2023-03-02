package it.cavallium.dbengine.database.collections;

import it.cavallium.dbengine.buffers.Buf;
import it.cavallium.dbengine.buffers.BufDataInput;
import it.cavallium.dbengine.buffers.BufDataOutput;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.Delta;
import it.cavallium.dbengine.database.LLDelta;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.LLDictionaryResultType;
import it.cavallium.dbengine.database.LLEntry;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.SerializedKey;
import it.cavallium.dbengine.database.SubStageEntry;
import it.cavallium.dbengine.database.UpdateMode;
import it.cavallium.dbengine.database.UpdateReturnMode;
import it.cavallium.dbengine.database.disk.BinarySerializationFunction;
import it.cavallium.dbengine.database.serialization.KVSerializationFunction;
import it.cavallium.dbengine.database.serialization.SerializationException;
import it.cavallium.dbengine.database.serialization.SerializationFunction;
import it.cavallium.dbengine.database.serialization.Serializer;
import it.cavallium.dbengine.database.serialization.SerializerFixedBinaryLength;
import it.cavallium.dbengine.utils.DBException;
import it.cavallium.dbengine.utils.StreamUtils;
import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectSortedMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectSortedMaps;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Optimized implementation of "DatabaseMapDictionary with SubStageGetterSingle"
 */
public class DatabaseMapDictionary<T, U> extends DatabaseMapDictionaryDeep<T, U, DatabaseStageEntry<U>> {

	private static final Logger LOG = LogManager.getLogger(DatabaseMapDictionary.class);

	private final AtomicLong totalZeroBytesErrors = new AtomicLong();
	private final Serializer<U> valueSerializer;

	protected DatabaseMapDictionary(LLDictionary dictionary,
			@Nullable Buf prefixKey,
			SerializerFixedBinaryLength<T> keySuffixSerializer,
			Serializer<U> valueSerializer) {
		// Do not retain or release or use the prefixKey here
		super(dictionary, prefixKey, keySuffixSerializer, new SubStageGetterSingle<>(valueSerializer), 0);
		this.valueSerializer = valueSerializer;
	}

	public static <T, U> DatabaseMapDictionary<T, U> simple(LLDictionary dictionary,
			SerializerFixedBinaryLength<T> keySerializer,
			Serializer<U> valueSerializer) {
		return new DatabaseMapDictionary<>(dictionary, null, keySerializer, valueSerializer);
	}

	public static <T, U> DatabaseMapDictionary<T, U> tail(LLDictionary dictionary,
			@Nullable Buf prefixKey,
			SerializerFixedBinaryLength<T> keySuffixSerializer,
			Serializer<U> valueSerializer) {
		return new DatabaseMapDictionary<>(dictionary, prefixKey, keySuffixSerializer, valueSerializer);
	}

	public static <K, V> Stream<Entry<K, V>> getLeavesFrom(DatabaseMapDictionary<K, V> databaseMapDictionary,
			CompositeSnapshot snapshot,
			@Nullable K keyMin,
			@Nullable K keyMax,
			boolean reverse,
			boolean smallRange) {

		if (keyMin != null || keyMax != null) {
			return databaseMapDictionary.getAllEntries(snapshot,
					keyMin,
					keyMax,
					reverse,
					smallRange,
					Map::entry
			);
		} else {
			return databaseMapDictionary.getAllEntries(snapshot, smallRange, Map::entry);
		}
	}

	public static <K> Stream<K> getKeyLeavesFrom(DatabaseMapDictionary<K, ?> databaseMapDictionary,
			CompositeSnapshot snapshot,
			@Nullable K keyMin,
			@Nullable K keyMax,
			boolean reverse,
			boolean smallRange) {

		Stream<? extends Entry<K, ? extends DatabaseStageEntry<?>>> stagesFlux;
		if (keyMin != null || keyMax != null) {
			stagesFlux = databaseMapDictionary.getAllStages(snapshot, keyMin, keyMax, reverse, smallRange);
		} else {
			stagesFlux = databaseMapDictionary.getAllStages(snapshot, smallRange);
		}
		return stagesFlux.map(Entry::getKey);
	}

	private @Nullable U deserializeValue(T keySuffix, BufDataInput value) {
		try {
			return valueSerializer.deserialize(value);
		} catch (IndexOutOfBoundsException ex) {
			var exMessage = ex.getMessage();
			if (exMessage != null && exMessage.contains("read 0 to 0, write 0 to ")) {
				var totalZeroBytesErrors = this.totalZeroBytesErrors.incrementAndGet();
				if (totalZeroBytesErrors < 512 || totalZeroBytesErrors % 10000 == 0) {
					var keySuffixBytes = serializeKeySuffixToKey(keySuffix);
					try {
						LOG.error(
								"Unexpected zero-bytes value at " + dictionary.getDatabaseName() + ":" + dictionary.getColumnName()
										+ ":" + LLUtils.toStringSafe(keyPrefix) + ":" + keySuffix + "(" + LLUtils.toStringSafe(
										keySuffixBytes) + ") total=" + totalZeroBytesErrors);
					} catch (SerializationException e) {
						LOG.error(
								"Unexpected zero-bytes value at " + dictionary.getDatabaseName() + ":" + dictionary.getColumnName()
										+ ":" + LLUtils.toStringSafe(keyPrefix) + ":" + keySuffix + "(?) total=" + totalZeroBytesErrors);
					}
				}
				return null;
			} else {
				throw ex;
			}
		}
	}

	private Buf serializeValue(U value) throws SerializationException {
		var valSizeHint = valueSerializer.getSerializedSizeHint();
		if (valSizeHint == -1) valSizeHint = 128;
		var valBuf = BufDataOutput.create(valSizeHint);
		try {
			valueSerializer.serialize(value, valBuf);
		} catch (SerializationException ex) {
			throw ex;
		} catch (Exception ex) {
			throw new SerializationException("Failed to serialize value");
		}
		return valBuf.asList();
	}

	private Buf serializeKeySuffixToKey(T keySuffix) throws SerializationException {
		BufDataOutput keyBuf = BufDataOutput.createLimited(keyPrefixLength + keySuffixLength + keyExtLength);
		if (keyPrefix != null) {
			keyBuf.writeBytes(keyPrefix);
		}
		assert keyBuf.size() == keyPrefixLength;
		serializeSuffixTo(keySuffix, keyBuf);
		assert keyBuf.size() == keyPrefixLength + keySuffixLength + keyExtLength;
		return keyBuf.asList();
	}

	private Buf toKey(Buf suffixKey) {
		assert suffixKeyLengthConsistency(suffixKey.size());
		if (keyPrefix != null) {
			var result = keyPrefix.copy();
			result.addAll(suffixKey);
			assert result.size() == keyPrefixLength + keySuffixLength + keyExtLength;
			return result;
		} else {
			assert suffixKey.size() == keyPrefixLength + keySuffixLength + keyExtLength;
			return suffixKey;
		}
	}

	@Override
	public Object2ObjectSortedMap<T, U> get(@Nullable CompositeSnapshot snapshot) {
		Stream<Entry<T, U>> stream = dictionary
				.getRange(resolveSnapshot(snapshot), range, false, true)
				.map(entry -> {
					Entry<T, U> deserializedEntry;
					T key;
					// serializedKey
					var buf1 = BufDataInput.create(entry.getKey());
					var serializedValue = BufDataInput.create(entry.getValue());
					// after this, it becomes serializedSuffixAndExt
					buf1.skipNBytes(keyPrefixLength);
					suffixAndExtKeyConsistency(buf1.available());

					key = deserializeSuffix(buf1);
					U value = valueSerializer.deserialize(serializedValue);
					deserializedEntry = Map.entry(key, value);
					return deserializedEntry;
				});
		// serializedKey
		// after this, it becomes serializedSuffixAndExt
		var map = StreamUtils.collect(stream,
				Collectors.toMap(Entry::getKey, Entry::getValue, (a, b) -> a, Object2ObjectLinkedOpenHashMap::new)
		);
		return map == null || map.isEmpty() ? null : map;
	}

	@Override
	public Object2ObjectSortedMap<T, U> setAndGetPrevious(Object2ObjectSortedMap<T, U> value) {
		Object2ObjectSortedMap<T, U> prev = this.get(null);
		if (value == null || value.isEmpty()) {
			dictionary.clear();
		} else {
			dictionary.setRange(range, value.entrySet().stream().map(this::serializeEntry), true);
		}
		return prev != null && prev.isEmpty() ? null : prev;
	}

	@Override
	public Object2ObjectSortedMap<T, U> clearAndGetPrevious() {
		return this.setAndGetPrevious(Object2ObjectSortedMaps.emptyMap());
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
	public @NotNull DatabaseStageEntry<U> at(@Nullable CompositeSnapshot snapshot, T keySuffix) {
		return new DatabaseMapSingle<>(dictionary, serializeKeySuffixToKey(keySuffix), valueSerializer);
	}

	@Override
	public boolean containsKey(@Nullable CompositeSnapshot snapshot, T keySuffix) {
		return !dictionary.isRangeEmpty(resolveSnapshot(snapshot),
				LLRange.single(serializeKeySuffixToKey(keySuffix)), true);
	}

	@Override
	public U getValue(@Nullable CompositeSnapshot snapshot, T keySuffix) {
		var keySuffixBuf = serializeKeySuffixToKey(keySuffix);
		Buf value = dictionary.get(resolveSnapshot(snapshot), keySuffixBuf);
		return value != null ? deserializeValue(keySuffix, BufDataInput.create(value)) : null;
	}

	@Override
	public void putValue(T keySuffix, U value) {
		var keyMono = serializeKeySuffixToKey(keySuffix);
		var valueMono = serializeValue(value);
		dictionary.put(keyMono, valueMono, LLDictionaryResultType.VOID);
	}

	@Override
	public UpdateMode getUpdateMode() {
		return dictionary.getUpdateMode();
	}

	@Override
	public U updateValue(T keySuffix,
			UpdateReturnMode updateReturnMode,
			SerializationFunction<@Nullable U, @Nullable U> updater) {
		var keyMono = serializeKeySuffixToKey(keySuffix);
		var result = dictionary.update(keyMono, getSerializedUpdater(updater), updateReturnMode);
		return deserializeValue(keySuffix, BufDataInput.create(result));
	}

	@Override
	public Delta<U> updateValueAndGetDelta(T keySuffix, SerializationFunction<@Nullable U, @Nullable U> updater) {
		var keyMono = serializeKeySuffixToKey(keySuffix);
		LLDelta delta = dictionary.updateAndGetDelta(keyMono, getSerializedUpdater(updater));
		return LLUtils.mapLLDelta(delta, in -> valueSerializer.deserialize(BufDataInput.create(in)));
	}

	public BinarySerializationFunction getSerializedUpdater(SerializationFunction<@Nullable U, @Nullable U> updater) {
		return oldSerialized -> {
			U result;
			if (oldSerialized == null) {
				result = updater.apply(null);
			} else {
				result = updater.apply(valueSerializer.deserialize(BufDataInput.create(oldSerialized)));
			}
			if (result == null) {
				return null;
			} else {
				return serializeValue(result);
			}
		};
	}

	public KVSerializationFunction<@NotNull T, @Nullable Buf, @Nullable Buf> getSerializedUpdater(
			KVSerializationFunction<@NotNull T, @Nullable U, @Nullable U> updater) {
		return (key, oldSerialized) -> {
			U result;
			if (oldSerialized == null) {
				result = updater.apply(key, null);
			} else {
				result = updater.apply(key, valueSerializer.deserialize(BufDataInput.create(oldSerialized)));
			}
			if (result == null) {
				return null;
			} else {
				return serializeValue(result);
			}
		};
	}

	@Override
	public U putValueAndGetPrevious(T keySuffix, U value) {
		var keyMono = serializeKeySuffixToKey(keySuffix);
		var valueMono = serializeValue(value);
		var valueBuf = dictionary.put(keyMono, valueMono, LLDictionaryResultType.PREVIOUS_VALUE);
		if (valueBuf == null) {
			return null;
		}
		return deserializeValue(keySuffix, BufDataInput.create(valueBuf));
	}

	@Override
	public boolean putValueAndGetChanged(T keySuffix, U value) {
		var keyMono = serializeKeySuffixToKey(keySuffix);
		var valueMono = serializeValue(value);
		var oldValueBuf = dictionary.put(keyMono, valueMono, LLDictionaryResultType.PREVIOUS_VALUE);
		var oldValue = oldValueBuf != null ? deserializeValue(keySuffix, BufDataInput.create(oldValueBuf)) : null;
		if (oldValue == null) {
			return value != null;
		} else {
			return !Objects.equals(oldValue, value);
		}
	}

	@Override
	public void remove(T keySuffix) {
		var keyMono = serializeKeySuffixToKey(keySuffix);
		dictionary.remove(keyMono, LLDictionaryResultType.VOID);
	}

	@Override
	public U removeAndGetPrevious(T keySuffix) {
		var keyMono = serializeKeySuffixToKey(keySuffix);
		var valueBuf = dictionary.remove(keyMono, LLDictionaryResultType.PREVIOUS_VALUE);
		return valueBuf != null ? deserializeValue(keySuffix, BufDataInput.create(valueBuf)) : null;
	}

	@Override
	public boolean removeAndGetStatus(T keySuffix) {
		var keyMono = serializeKeySuffixToKey(keySuffix);
		return LLUtils.responseToBoolean(dictionary.remove(keyMono, LLDictionaryResultType.PREVIOUS_VALUE_EXISTENCE));
	}

	@Override
	public Stream<Optional<U>> getMulti(@Nullable CompositeSnapshot snapshot, Stream<T> keys) {
		var mappedKeys = keys.map(keySuffix -> serializeKeySuffixToKey(keySuffix));
		return dictionary
				.getMulti(resolveSnapshot(snapshot), mappedKeys)
				.map(valueBufOpt -> {
					if (valueBufOpt.isPresent()) {
						return Optional.of(valueSerializer.deserialize(BufDataInput.create(valueBufOpt.get())));
					} else {
						return Optional.empty();
					}
				});
	}

	private LLEntry serializeEntry(T keySuffix, U value) throws SerializationException {
		var key = serializeKeySuffixToKey(keySuffix);
		var serializedValue = serializeValue(value);
		return LLEntry.of(key, serializedValue);
	}

	private LLEntry serializeEntry(Entry<T, U> entry) throws SerializationException {
		return serializeEntry(entry.getKey(), entry.getValue());
	}

	@Override
	public void putMulti(Stream<Entry<T, U>> entries) {
		try (var serializedEntries = entries.map(entry -> serializeEntry(entry))) {
			dictionary.putMulti(serializedEntries);
		}
	}

	@Override
	public Stream<Boolean> updateMulti(Stream<T> keys,
			KVSerializationFunction<T, @Nullable U, @Nullable U> updater) {
		var serializedKeys = keys.map(keySuffix -> new SerializedKey<>(keySuffix, serializeKeySuffixToKey(keySuffix)));
		var serializedUpdater = getSerializedUpdater(updater);
		return dictionary.updateMulti(serializedKeys, serializedUpdater);
	}

	@Override
	public Stream<SubStageEntry<T, DatabaseStageEntry<U>>> getAllStages(@Nullable CompositeSnapshot snapshot, boolean smallRange) {
		return getAllStages(snapshot, range, false, smallRange);
	}

	private LLRange getPatchedRange(@NotNull LLRange range, @Nullable T keyMin, @Nullable T keyMax)
			throws SerializationException {
		Buf keyMinBuf = serializeSuffixForRange(keyMin);
		if (keyMinBuf == null) {
			keyMinBuf = range.getMin();
		}
		Buf keyMaxBuf = serializeSuffixForRange(keyMax);
		if (keyMaxBuf == null) {
			keyMaxBuf = range.getMax();
		}
		return LLRange.of(keyMinBuf, keyMaxBuf);
	}

	private Buf serializeSuffixForRange(@Nullable T key) throws SerializationException {
		if (key == null) {
			return null;
		}
		var keyWithoutExtBuf = BufDataOutput.createLimited(keyPrefixLength + keySuffixLength);
		if (keyPrefix != null) {
			keyWithoutExtBuf.writeBytes(keyPrefix);
		}
		serializeSuffixTo(key, keyWithoutExtBuf);
		return keyWithoutExtBuf.asList();
	}

	/**
	 * Get all stages
	 * @param reverse if true, the results will go backwards from the specified key (inclusive)
	 */
	public Stream<SubStageEntry<T, DatabaseStageEntry<U>>> getAllStages(@Nullable CompositeSnapshot snapshot,
			@Nullable T keyMin,
			@Nullable T keyMax,
			boolean reverse,
			boolean smallRange) {
		if (keyMin == null && keyMax == null) {
			return getAllStages(snapshot, smallRange);
		} else {
			LLRange boundedRange = getPatchedRange(range, keyMin, keyMax);
			return getAllStages(snapshot, boundedRange, reverse, smallRange);
		}
	}

	private Stream<SubStageEntry<T, DatabaseStageEntry<U>>> getAllStages(@Nullable CompositeSnapshot snapshot,
			LLRange sliceRange, boolean reverse, boolean smallRange) {
		return dictionary
				.getRangeKeys(resolveSnapshot(snapshot), sliceRange, reverse, smallRange)
				.map(keyBuf -> {
					assert keyBuf.size() == keyPrefixLength + keySuffixLength + keyExtLength;
					// Remove prefix. Keep only the suffix and the ext
					var suffixAndExtIn = BufDataInput.create(keyBuf);
					suffixAndExtIn.skipBytes(keyPrefixLength);

					suffixKeyLengthConsistency(suffixAndExtIn.available());
					T keySuffix = deserializeSuffix(suffixAndExtIn);
					var subStage = new DatabaseMapSingle<>(dictionary, keyBuf, valueSerializer);
					return new SubStageEntry<>(keySuffix, subStage);
				});
	}

	private Stream<T> getAllKeys(@Nullable CompositeSnapshot snapshot,
			LLRange sliceRange, boolean reverse, boolean smallRange) {
		return dictionary
				.getRangeKeys(resolveSnapshot(snapshot), sliceRange, reverse, smallRange)
				.map(keyBuf -> {
					assert keyBuf.size() == keyPrefixLength + keySuffixLength + keyExtLength;
					// Remove prefix. Keep only the suffix and the ext
					var suffixAndExtIn = BufDataInput.create(keyBuf);
					suffixAndExtIn.skipBytes(keyPrefixLength);

					suffixKeyLengthConsistency(suffixAndExtIn.available());
					return deserializeSuffix(suffixAndExtIn);
				});
	}

	@Override
	public Stream<Entry<T, U>> getAllEntries(@Nullable CompositeSnapshot snapshot, boolean smallRange) {
		return getAllEntries(snapshot, smallRange, Map::entry);
	}

	@Override
	public Stream<U> getAllValues(@Nullable CompositeSnapshot snapshot, boolean smallRange) {
		return getAllEntries(snapshot, range, false, smallRange, (k, v) -> v);
	}

	@Override
	public Stream<T> getAllKeys(@Nullable CompositeSnapshot snapshot, boolean smallRange) {
		return getAllKeys(snapshot, range, false, smallRange);
	}

	/**
	 * Get all values
	 * @param reverse if true, the results will go backwards from the specified key (inclusive)
	 */
	public Stream<Entry<T, U>> getAllEntries(@Nullable CompositeSnapshot snapshot,
			@Nullable T keyMin,
			@Nullable T keyMax,
			boolean reverse,
			boolean smallRange) {
		return getAllEntries(snapshot, keyMin, keyMax, reverse, smallRange, Map::entry);
	}

	/**
	 * Get all values
	 * @param reverse if true, the results will go backwards from the specified key (inclusive)
	 */
	public <X> Stream<X> getAllEntries(@Nullable CompositeSnapshot snapshot,
			@Nullable T keyMin,
			@Nullable T keyMax,
			boolean reverse,
			boolean smallRange,
			BiFunction<T, U, X> mapper) {
		if (keyMin == null && keyMax == null) {
			return getAllEntries(snapshot, smallRange, mapper);
		} else {
			LLRange boundedRange = getPatchedRange(range, keyMin, keyMax);
			return getAllEntries(snapshot, boundedRange, reverse, smallRange, mapper);
		}
	}

	private <X> Stream<X> getAllEntries(@Nullable CompositeSnapshot snapshot, boolean smallRange, BiFunction<T, U, X> mapper) {
		return getAllEntries(snapshot, range, false, smallRange, mapper);
	}

	private <X> Stream<X> getAllEntries(@Nullable CompositeSnapshot snapshot,
			LLRange sliceRangeMono,
			boolean reverse,
			boolean smallRange,
			BiFunction<T, U, X> mapper) {
		return dictionary
				.getRange(resolveSnapshot(snapshot), sliceRangeMono, reverse, smallRange)
				.map((serializedEntry) -> {
					X entry;
					var keyBuf = serializedEntry.getKey();
					assert keyBuf != null;
					assert keyBuf.size() == keyPrefixLength + keySuffixLength + keyExtLength;

					// Remove prefix. Keep only the suffix and the ext
					var suffixAndExtIn = BufDataInput.create(keyBuf);
					suffixAndExtIn.skipBytes(keyPrefixLength);

					assert suffixKeyLengthConsistency(suffixAndExtIn.available());
					T keySuffix = deserializeSuffix(suffixAndExtIn);

					assert serializedEntry.getValue() != null;
					U value = valueSerializer.deserialize(BufDataInput.create(serializedEntry.getValue()));
					entry = mapper.apply(keySuffix, value);
					return entry;
				});
	}

	@Override
	public Stream<Entry<T, U>> setAllEntriesAndGetPrevious(Stream<Entry<T, U>> entries) {
		return getAllEntries(null, false)
				.onClose(() -> dictionary.setRange(range, entries.map(entry -> serializeEntry(entry)), false));
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

}
