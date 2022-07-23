package it.cavallium.dbengine.database.collections;

import static java.util.Objects.requireNonNullElseGet;

import io.netty5.buffer.api.Buffer;
import io.netty5.util.Resource;
import io.netty5.buffer.api.internal.ResourceSupport;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.BufSupplier;
import it.cavallium.dbengine.database.Delta;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.LLDictionaryResultType;
import it.cavallium.dbengine.database.LLEntry;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.SubStageEntry;
import it.cavallium.dbengine.database.UpdateMode;
import it.cavallium.dbengine.database.UpdateReturnMode;
import it.cavallium.dbengine.database.disk.BinarySerializationFunction;
import it.cavallium.dbengine.database.serialization.KVSerializationFunction;
import it.cavallium.dbengine.database.serialization.SerializationException;
import it.cavallium.dbengine.database.serialization.SerializationFunction;
import it.cavallium.dbengine.database.serialization.Serializer;
import it.cavallium.dbengine.database.serialization.SerializerFixedBinaryLength;
import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectSortedMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectSortedMaps;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Optimized implementation of "DatabaseMapDictionary with SubStageGetterSingle"
 */
public class DatabaseMapDictionary<T, U> extends DatabaseMapDictionaryDeep<T, U, DatabaseStageEntry<U>> {

	private static final Logger LOG = LogManager.getLogger(DatabaseMapDictionary.class);

	private final AtomicLong totalZeroBytesErrors = new AtomicLong();
	private final Serializer<U> valueSerializer;

	protected DatabaseMapDictionary(LLDictionary dictionary,
			@Nullable BufSupplier prefixKeySupplier,
			SerializerFixedBinaryLength<T> keySuffixSerializer,
			Serializer<U> valueSerializer) {
		// Do not retain or release or use the prefixKey here
		super(dictionary, prefixKeySupplier, keySuffixSerializer, new SubStageGetterSingle<>(valueSerializer), 0);
		this.valueSerializer = valueSerializer;
	}

	public static <T, U> DatabaseMapDictionary<T, U> simple(LLDictionary dictionary,
			SerializerFixedBinaryLength<T> keySerializer,
			Serializer<U> valueSerializer) {
		return new DatabaseMapDictionary<>(dictionary, null, keySerializer, valueSerializer);
	}

	public static <T, U> DatabaseMapDictionary<T, U> tail(LLDictionary dictionary,
			@Nullable BufSupplier prefixKeySupplier,
			SerializerFixedBinaryLength<T> keySuffixSerializer,
			Serializer<U> valueSerializer) {
		return new DatabaseMapDictionary<>(dictionary, prefixKeySupplier, keySuffixSerializer, valueSerializer);
	}

	public static <K, V> Flux<Entry<K, V>> getLeavesFrom(DatabaseMapDictionary<K, V> databaseMapDictionary,
			CompositeSnapshot snapshot,
			Mono<K> keyMin,
			Mono<K> keyMax,
			boolean reverse, boolean smallRange) {
		Mono<Optional<K>> keyMinOptMono = keyMin.map(Optional::of).defaultIfEmpty(Optional.empty());
		Mono<Optional<K>> keyMaxOptMono = keyMax.map(Optional::of).defaultIfEmpty(Optional.empty());

		return Mono.zip(keyMinOptMono, keyMaxOptMono).flatMapMany(entry -> {
			var keyMinOpt = entry.getT1();
			var keyMaxOpt = entry.getT2();
			if (keyMinOpt.isPresent() || keyMaxOpt.isPresent()) {
				return databaseMapDictionary.getAllValues(snapshot,
						keyMinOpt.orElse(null),
						keyMaxOpt.orElse(null),
						reverse,
						smallRange
				);
			} else {
				return databaseMapDictionary.getAllValues(snapshot, smallRange);
			}
		});
	}

	public static <K> Flux<K> getKeyLeavesFrom(DatabaseMapDictionary<K, ?> databaseMapDictionary,
			CompositeSnapshot snapshot,
			Mono<K> keyMin,
			Mono<K> keyMax,
			boolean reverse, boolean smallRange) {
		Mono<Optional<K>> keyMinOptMono = keyMin.map(Optional::of).defaultIfEmpty(Optional.empty());
		Mono<Optional<K>> keyMaxOptMono = keyMax.map(Optional::of).defaultIfEmpty(Optional.empty());

		return Mono.zip(keyMinOptMono, keyMaxOptMono).flatMapMany(keys -> {
			var keyMinOpt = keys.getT1();
			var keyMaxOpt = keys.getT2();
			Flux<? extends Entry<K, ? extends DatabaseStageEntry<?>>> stagesFlux;
			if (keyMinOpt.isPresent() || keyMaxOpt.isPresent()) {
				stagesFlux = databaseMapDictionary
						.getAllStages(snapshot, keyMinOpt.orElse(null), keyMaxOpt.orElse(null), reverse, smallRange);
			} else {
				stagesFlux = databaseMapDictionary.getAllStages(snapshot, smallRange);
			}
			return stagesFlux.doOnNext(e -> e.getValue().close())
					.doOnDiscard(Entry.class, e -> {
						if (e.getValue() instanceof DatabaseStageEntry<?> resource) {
							LLUtils.onDiscard(resource);
						}
					})
					.map(Entry::getKey);
		});
	}

	private @Nullable U deserializeValue(T keySuffix, Buffer value) {
		try {
			return valueSerializer.deserialize(value);
		} catch (IndexOutOfBoundsException ex) {
			var exMessage = ex.getMessage();
			if (exMessage != null && exMessage.contains("read 0 to 0, write 0 to ")) {
				var totalZeroBytesErrors = this.totalZeroBytesErrors.incrementAndGet();
				if (totalZeroBytesErrors < 512 || totalZeroBytesErrors % 10000 == 0) {
					try (var keyPrefix = keyPrefixSupplier.get()) {
						try (var keySuffixBytes = serializeKeySuffixToKey(keySuffix)) {
							LOG.error(
									"Unexpected zero-bytes value at "
											+ dictionary.getDatabaseName() + ":" + dictionary.getColumnName()
											+ ":" + LLUtils.toStringSafe(keyPrefix) + ":" + keySuffix
											+ "(" + LLUtils.toStringSafe(keySuffixBytes) + ") total=" + totalZeroBytesErrors);
						} catch (SerializationException e) {
							LOG.error(
									"Unexpected zero-bytes value at " + dictionary.getDatabaseName() + ":" + dictionary.getColumnName()
											+ ":" + LLUtils.toStringSafe(keyPrefix) + ":" + keySuffix + "(?) total="
											+ totalZeroBytesErrors);
						}
					}
				}
				return null;
			} else {
				throw ex;
			}
		}
	}

	private Buffer serializeValue(U value) throws SerializationException {
		var valSizeHint = valueSerializer.getSerializedSizeHint();
		if (valSizeHint == -1) valSizeHint = 128;
		var valBuf = dictionary.getAllocator().allocate(valSizeHint);
		try {
			valueSerializer.serialize(value, valBuf);
			return valBuf;
		} catch (Throwable t) {
			valBuf.close();
			throw t;
		}
	}

	private Buffer serializeKeySuffixToKey(T keySuffix) throws SerializationException {
		Buffer keyBuf;
		if (keyPrefixSupplier != null) {
			keyBuf = keyPrefixSupplier.get();
		} else {
			keyBuf = this.dictionary.getAllocator().allocate(keyPrefixLength + keySuffixLength + keyExtLength);
		}
		try {
			assert keyBuf.readableBytes() == keyPrefixLength;
			keyBuf.ensureWritable(keySuffixLength + keyExtLength);
			serializeSuffix(keySuffix, keyBuf);
			assert keyBuf.readableBytes() == keyPrefixLength + keySuffixLength + keyExtLength;
			return keyBuf;
		} catch (Throwable t) {
			keyBuf.close();
			throw t;
		}
	}

	private Buffer toKey(Buffer suffixKey) {
		assert suffixKeyLengthConsistency(suffixKey.readableBytes());
		if (keyPrefixSupplier != null) {
			var result = LLUtils.compositeBuffer(dictionary.getAllocator(), keyPrefixSupplier.get().send(), suffixKey.send());
			try {
				assert result.readableBytes() == keyPrefixLength + keySuffixLength + keyExtLength;
				return result;
			} catch (Throwable t) {
				result.close();
				throw t;
			}
		} else {
			assert suffixKey.readableBytes() == keyPrefixLength + keySuffixLength + keyExtLength;
			return suffixKey;
		}
	}

	@Override
	public Mono<Object2ObjectSortedMap<T, U>> get(@Nullable CompositeSnapshot snapshot) {
		return dictionary
				.getRange(resolveSnapshot(snapshot), rangeMono, false, true)
				.map(entry -> {
					Entry<T, U> deserializedEntry;
					try (entry) {
						T key;
						var serializedKey = entry.getKeyUnsafe();
						var serializedValue = entry.getValueUnsafe();
						splitPrefix(serializedKey).close();
						suffixKeyLengthConsistency(serializedKey.readableBytes());
						key = deserializeSuffix(serializedKey);
						U value = valueSerializer.deserialize(serializedValue);
						deserializedEntry = Map.entry(key, value);
					}
					return deserializedEntry;
				})
				.collectMap(Entry::getKey, Entry::getValue, Object2ObjectLinkedOpenHashMap::new)
				.map(map -> (Object2ObjectSortedMap<T, U>) map)
				.filter(map -> !map.isEmpty());
	}

	@Override
	public Mono<Object2ObjectSortedMap<T, U>> setAndGetPrevious(Object2ObjectSortedMap<T, U> value) {
		return this
				.get(null)
				.concatWith(dictionary.setRange(rangeMono, Flux
						.fromIterable(Collections.unmodifiableMap(value).entrySet())
						.map(entry -> serializeEntry(entry)), true).then(Mono.empty()))
				.singleOrEmpty();
	}

	@Override
	public Mono<Object2ObjectSortedMap<T, U>> clearAndGetPrevious() {
		return this
				.setAndGetPrevious(Object2ObjectSortedMaps.emptyMap());
	}

	@Override
	public Mono<Long> leavesCount(@Nullable CompositeSnapshot snapshot, boolean fast) {
		return dictionary.sizeRange(resolveSnapshot(snapshot), rangeMono, fast);
	}

	@Override
	public Mono<Boolean> isEmpty(@Nullable CompositeSnapshot snapshot) {
		return dictionary.isRangeEmpty(resolveSnapshot(snapshot), rangeMono, false);
	}

	@Override
	public Mono<DatabaseStageEntry<U>> at(@Nullable CompositeSnapshot snapshot, T keySuffix) {
		return Mono.fromCallable(() ->
				new DatabaseMapSingle<>(dictionary, BufSupplier.ofOwned(serializeKeySuffixToKey(keySuffix)), valueSerializer));
	}

	@Override
	public Mono<Boolean> containsKey(@Nullable CompositeSnapshot snapshot, T keySuffix) {
		return dictionary
				.isRangeEmpty(resolveSnapshot(snapshot),
						Mono.fromCallable(() -> LLRange.singleUnsafe(serializeKeySuffixToKey(keySuffix))),
						true
				)
				.map(empty -> !empty);
	}

	@Override
	public Mono<U> getValue(@Nullable CompositeSnapshot snapshot, T keySuffix) {
		return Mono.usingWhen(dictionary
				.get(resolveSnapshot(snapshot), Mono.fromCallable(() -> serializeKeySuffixToKey(keySuffix))),
				value -> Mono.fromCallable(() -> deserializeValue(keySuffix, value)),
				LLUtils::finalizeResource);
	}

	@Override
	public Mono<Void> putValue(T keySuffix, U value) {
		var keyMono = Mono.fromCallable(() -> serializeKeySuffixToKey(keySuffix)).single();
		var valueMono = Mono.fromCallable(() -> serializeValue(value)).single();
		return Mono.usingWhen(dictionary.put(keyMono, valueMono, LLDictionaryResultType.VOID),
				v -> Mono.empty(),
				LLUtils::finalizeResource
		);
	}

	@Override
	public Mono<UpdateMode> getUpdateMode() {
		return dictionary.getUpdateMode();
	}

	@Override
	public Mono<U> updateValue(T keySuffix, UpdateReturnMode updateReturnMode,
			SerializationFunction<@Nullable U, @Nullable U> updater) {
		var keyMono = Mono.fromCallable(() -> serializeKeySuffixToKey(keySuffix));
		return Mono.usingWhen(dictionary.update(keyMono, getSerializedUpdater(updater), updateReturnMode),
				result -> Mono.fromCallable(() -> deserializeValue(keySuffix, result)),
				LLUtils::finalizeResource
		);
	}

	@Override
	public Mono<Delta<U>> updateValueAndGetDelta(T keySuffix, SerializationFunction<@Nullable U, @Nullable U> updater) {
		var keyMono = Mono.fromCallable(() -> serializeKeySuffixToKey(keySuffix));
		return dictionary
				.updateAndGetDelta(keyMono, getSerializedUpdater(updater))
				.transform(mono -> LLUtils.mapLLDelta(mono, serialized -> valueSerializer.deserialize(serialized)));
	}

	public BinarySerializationFunction getSerializedUpdater(SerializationFunction<@Nullable U, @Nullable U> updater) {
		return oldSerialized -> {
			U result;
			if (oldSerialized == null) {
				result = updater.apply(null);
			} else {
				try (oldSerialized) {
					result = updater.apply(valueSerializer.deserialize(oldSerialized));
				}
			}
			if (result == null) {
				return null;
			} else {
				return serializeValue(result);
			}
		};
	}

	public KVSerializationFunction<@NotNull T, @Nullable Buffer, @Nullable Buffer> getSerializedUpdater(
			KVSerializationFunction<@NotNull T, @Nullable U, @Nullable U> updater) {
		return (key, oldSerialized) -> {
			try (oldSerialized) {
				U result;
				if (oldSerialized == null) {
					result = updater.apply(key, null);
				} else {
					try (oldSerialized) {
						result = updater.apply(key, valueSerializer.deserialize(oldSerialized));
					}
				}
				if (result == null) {
					return null;
				} else {
					return serializeValue(result);
				}
			}
		};
	}

	@Override
	public Mono<U> putValueAndGetPrevious(T keySuffix, U value) {
		var keyMono = Mono.fromCallable(() -> serializeKeySuffixToKey(keySuffix));
		var valueMono = Mono.fromCallable(() -> serializeValue(value));
		return Mono.usingWhen(dictionary.put(keyMono, valueMono, LLDictionaryResultType.PREVIOUS_VALUE),
				valueBuf -> Mono.fromCallable(() -> deserializeValue(keySuffix, valueBuf)),
				LLUtils::finalizeResource
		);
	}

	@Override
	public Mono<Boolean> putValueAndGetChanged(T keySuffix, U value) {
		var keyMono = Mono.fromCallable(() -> serializeKeySuffixToKey(keySuffix));
		var valueMono = Mono.fromCallable(() -> serializeValue(value));
		return Mono
				.usingWhen(dictionary.put(keyMono, valueMono, LLDictionaryResultType.PREVIOUS_VALUE),
						valueBuf -> Mono.fromCallable(() -> deserializeValue(keySuffix, valueBuf)),
						LLUtils::finalizeResource
				)
				.map(oldValue -> !Objects.equals(oldValue, value))
				.defaultIfEmpty(value != null);
	}

	@Override
	public Mono<Void> remove(T keySuffix) {
		var keyMono = Mono.fromCallable(() -> serializeKeySuffixToKey(keySuffix));
		return dictionary
				.remove(keyMono, LLDictionaryResultType.VOID)
				.doOnNext(LLUtils::finalizeResourceNow)
				.then();
	}

	@Override
	public Mono<U> removeAndGetPrevious(T keySuffix) {
		var keyMono = Mono.fromCallable(() -> serializeKeySuffixToKey(keySuffix));
		return Mono.usingWhen(dictionary.remove(keyMono, LLDictionaryResultType.PREVIOUS_VALUE),
				valueBuf -> Mono.fromCallable(() -> deserializeValue(keySuffix, valueBuf)),
				LLUtils::finalizeResource
		);
	}

	@Override
	public Mono<Boolean> removeAndGetStatus(T keySuffix) {
		var keyMono = Mono.fromCallable(() -> serializeKeySuffixToKey(keySuffix));
		return dictionary
				.remove(keyMono, LLDictionaryResultType.PREVIOUS_VALUE_EXISTENCE)
				.map(response -> LLUtils.responseToBoolean(response));
	}

	@Override
	public Flux<Optional<U>> getMulti(@Nullable CompositeSnapshot snapshot, Flux<T> keys) {
		var mappedKeys = keys.map(keySuffix -> serializeKeySuffixToKey(keySuffix));
		return dictionary
				.getMulti(resolveSnapshot(snapshot), mappedKeys)
				.map(valueBufOpt -> {
					try (valueBufOpt) {
						if (valueBufOpt.isPresent()) {
							return Optional.of(valueSerializer.deserialize(valueBufOpt.get()));
						} else {
							return Optional.empty();
						}
					}
				});
	}

	private LLEntry serializeEntry(T keySuffix, U value) throws SerializationException {
		var key = serializeKeySuffixToKey(keySuffix);
		try {
			var serializedValue = serializeValue(value);
			return LLEntry.of(key, serializedValue);
		} catch (Throwable t) {
			key.close();
			throw t;
		}
	}

	private LLEntry serializeEntry(Entry<T, U> entry) throws SerializationException {
		return serializeEntry(entry.getKey(), entry.getValue());
	}

	@Override
	public Mono<Void> putMulti(Flux<Entry<T, U>> entries) {
		var serializedEntries = entries.map(entry -> serializeEntry(entry));
		return dictionary.putMulti(serializedEntries);
	}

	@Override
	public Flux<Boolean> updateMulti(Flux<T> keys,
			KVSerializationFunction<T, @Nullable U, @Nullable U> updater) {
		var sharedKeys = keys.publish().refCount(2);
		var serializedKeys = sharedKeys.map(keySuffix -> serializeKeySuffixToKey(keySuffix));
		var serializedUpdater = getSerializedUpdater(updater);
		return dictionary.updateMulti(sharedKeys, serializedKeys, serializedUpdater);
	}

	@Override
	public Flux<SubStageEntry<T, DatabaseStageEntry<U>>> getAllStages(@Nullable CompositeSnapshot snapshot, boolean smallRange) {
		return getAllStages(snapshot, rangeMono, false, smallRange);
	}

	private LLRange getPatchedRange(@NotNull LLRange range, @Nullable T keyMin, @Nullable T keyMax)
			throws SerializationException {
		Buffer keyMinBuf = serializeSuffixForRange(keyMin);
		if (keyMinBuf == null) {
			keyMinBuf = range.getMinCopy();
		}
		Buffer keyMaxBuf = serializeSuffixForRange(keyMax);
		if (keyMaxBuf == null) {
			keyMaxBuf = range.getMaxCopy();
		}
		return LLRange.ofUnsafe(keyMinBuf, keyMaxBuf);
	}

	private Buffer serializeSuffixForRange(@Nullable T key) throws SerializationException {
		if (key == null) {
			return null;
		}
		var keyWithoutExtBuf =
				keyPrefixSupplier == null ? alloc.allocate(keySuffixLength + keyExtLength) : keyPrefixSupplier.get();
		try {
			keyWithoutExtBuf.ensureWritable(keySuffixLength + keyExtLength);
			serializeSuffix(key, keyWithoutExtBuf);
			return keyWithoutExtBuf;
		} catch (Throwable ex) {
			keyWithoutExtBuf.close();
			throw ex;
		}
	}

	/**
	 * Get all stages
	 * @param reverse if true, the results will go backwards from the specified key (inclusive)
	 */
	public Flux<SubStageEntry<T, DatabaseStageEntry<U>>> getAllStages(@Nullable CompositeSnapshot snapshot,
			@Nullable T keyMin,
			@Nullable T keyMax,
			boolean reverse,
			boolean smallRange) {
		if (keyMin == null && keyMax == null) {
			return getAllStages(snapshot, smallRange);
		} else {
			Mono<LLRange> boundedRangeMono = rangeMono.map(range -> {
				try (range) {
					return getPatchedRange(range, keyMin, keyMax);
				}
			});
			return getAllStages(snapshot, boundedRangeMono, reverse, smallRange);
		}
	}

	private Flux<SubStageEntry<T, DatabaseStageEntry<U>>> getAllStages(@Nullable CompositeSnapshot snapshot,
			Mono<LLRange> sliceRangeMono, boolean reverse, boolean smallRange) {
		return dictionary
				.getRangeKeys(resolveSnapshot(snapshot), sliceRangeMono, reverse, smallRange)
				.map(keyBuf -> {
					try (keyBuf) {
						assert keyBuf.readableBytes() == keyPrefixLength + keySuffixLength + keyExtLength;
						// Remove prefix. Keep only the suffix and the ext
						splitPrefix(keyBuf).close();
						suffixKeyLengthConsistency(keyBuf.readableBytes());
						var bufSupplier = BufSupplier.ofOwned(toKey(keyBuf.copy()));
						try {
							T keySuffix = deserializeSuffix(keyBuf);
							var subStage = new DatabaseMapSingle<>(dictionary, bufSupplier, valueSerializer);
							return new SubStageEntry<>(keySuffix, subStage);
						} catch (Throwable ex) {
							bufSupplier.close();
							throw ex;
						}
					}
				});
	}

	@Override
	public Flux<Entry<T, U>> getAllValues(@Nullable CompositeSnapshot snapshot, boolean smallRange) {
		return getAllValues(snapshot, rangeMono, false, smallRange);
	}

	/**
	 * Get all values
	 * @param reverse if true, the results will go backwards from the specified key (inclusive)
	 */
	public Flux<Entry<T, U>> getAllValues(@Nullable CompositeSnapshot snapshot,
			@Nullable T keyMin,
			@Nullable T keyMax,
			boolean reverse,
			boolean smallRange) {
		if (keyMin == null && keyMax == null) {
			return getAllValues(snapshot, smallRange);
		} else {
			Mono<LLRange> boundedRangeMono = Mono.usingWhen(rangeMono,
					range -> Mono.fromCallable(() -> getPatchedRange(range, keyMin, keyMax)),
					LLUtils::finalizeResource);
			return getAllValues(snapshot, boundedRangeMono, reverse, smallRange);
		}
	}

	private Flux<Entry<T, U>> getAllValues(@Nullable CompositeSnapshot snapshot,
			Mono<LLRange> sliceRangeMono,
			boolean reverse, boolean smallRange) {
		return dictionary
				.getRange(resolveSnapshot(snapshot), sliceRangeMono, reverse, smallRange)
				.map((serializedEntry) -> {
					Entry<T, U> entry;
					try (serializedEntry) {
						var keyBuf = serializedEntry.getKeyUnsafe();
						assert keyBuf != null;
						assert keyBuf.readableBytes() == keyPrefixLength + keySuffixLength + keyExtLength;
						// Remove prefix. Keep only the suffix and the ext
						splitPrefix(keyBuf).close();
						assert suffixKeyLengthConsistency(keyBuf.readableBytes());
						T keySuffix = deserializeSuffix(keyBuf);

						assert serializedEntry.getValueUnsafe() != null;
						U value = valueSerializer.deserialize(serializedEntry.getValueUnsafe());
						entry = Map.entry(keySuffix, value);
					}
					return entry;
				});
	}

	@Override
	public Flux<Entry<T, U>> setAllValuesAndGetPrevious(Flux<Entry<T, U>> entries) {
		return Flux.usingWhen(Mono.just(true),
				b -> this.getAllValues(null, false),
				b -> dictionary.setRange(rangeMono, entries.map(entry -> serializeEntry(entry)), false)
		);
	}

	@Override
	public Mono<Void> clear() {
		return Mono.using(() -> rangeSupplier.get(), range -> {
			if (range.isAll()) {
				return dictionary.clear();
			} else if (range.isSingle()) {
				return dictionary
						.remove(Mono.fromCallable(() -> range.getSingleUnsafe()), LLDictionaryResultType.VOID)
						.doOnNext(LLUtils::finalizeResourceNow)
						.then();
			} else {
				return dictionary.setRange(rangeMono, Flux.empty(), false);
			}
		}, LLUtils::finalizeResourceNow);
	}

}
