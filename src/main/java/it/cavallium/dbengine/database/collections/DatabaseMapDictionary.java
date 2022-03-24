package it.cavallium.dbengine.database.collections;

import io.netty5.buffer.api.Buffer;
import io.netty5.buffer.api.Resource;
import io.netty5.buffer.api.Send;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.Delta;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.LLDictionaryResultType;
import it.cavallium.dbengine.database.LLEntry;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.UpdateMode;
import it.cavallium.dbengine.database.UpdateReturnMode;
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
import reactor.core.publisher.SynchronousSink;

/**
 * Optimized implementation of "DatabaseMapDictionary with SubStageGetterSingle"
 */
public class DatabaseMapDictionary<T, U> extends DatabaseMapDictionaryDeep<T, U, DatabaseStageEntry<U>> {

	private static final Logger LOG = LogManager.getLogger(DatabaseMapDictionary.class);

	private final AtomicLong totalZeroBytesErrors = new AtomicLong();
	private final Serializer<U> valueSerializer;

	protected DatabaseMapDictionary(LLDictionary dictionary,
			@Nullable Buffer prefixKey,
			SerializerFixedBinaryLength<T> keySuffixSerializer,
			Serializer<U> valueSerializer,
			Runnable onClose) {
		// Do not retain or release or use the prefixKey here
		super(dictionary, prefixKey, keySuffixSerializer, new SubStageGetterSingle<>(valueSerializer), 0, onClose);
		this.valueSerializer = valueSerializer;
	}

	public static <T, U> DatabaseMapDictionary<T, U> simple(LLDictionary dictionary,
			SerializerFixedBinaryLength<T> keySerializer,
			Serializer<U> valueSerializer,
			Runnable onClose) {
		return new DatabaseMapDictionary<>(dictionary, null, keySerializer,
				valueSerializer, onClose);
	}

	public static <T, U> DatabaseMapDictionary<T, U> tail(LLDictionary dictionary,
			@Nullable Buffer prefixKey,
			SerializerFixedBinaryLength<T> keySuffixSerializer,
			Serializer<U> valueSerializer,
			Runnable onClose) {
		return new DatabaseMapDictionary<>(dictionary, prefixKey, keySuffixSerializer, valueSerializer, onClose);
	}

	public static <K, V> Flux<Entry<K, V>> getLeavesFrom(DatabaseMapDictionary<K, V> databaseMapDictionary,
			CompositeSnapshot snapshot,
			Mono<K> key,
			boolean reverse) {
		Mono<Optional<K>> keyOptMono = key.map(Optional::of).defaultIfEmpty(Optional.empty());

		return keyOptMono.flatMapMany(keyOpt -> {
			if (keyOpt.isPresent()) {
				return databaseMapDictionary.getAllValues(snapshot, keyOpt.get(), reverse);
			} else {
				return databaseMapDictionary.getAllValues(snapshot);
			}
		});
	}

	public static <K> Flux<K> getKeyLeavesFrom(DatabaseMapDictionary<K, ?> databaseMapDictionary,
			CompositeSnapshot snapshot,
			Mono<K> key,
			boolean reverse) {
		Mono<Optional<K>> keyOptMono = key.map(Optional::of).defaultIfEmpty(Optional.empty());

		return keyOptMono.flatMapMany(keyOpt -> {
			Flux<? extends Entry<K, ? extends DatabaseStageEntry<?>>> stagesFlux;
			if (keyOpt.isPresent()) {
				stagesFlux = databaseMapDictionary
						.getAllStages(snapshot, keyOpt.get(), reverse);
			} else {
				stagesFlux = databaseMapDictionary.getAllStages(snapshot);
			}
			return stagesFlux.doOnNext(e -> e.getValue().close())
					.doOnDiscard(Entry.class, e -> {
						if (e.getValue() instanceof DatabaseStageEntry<?> resource) {
							resource.close();
						}
					})
					.map(Entry::getKey);
		});
	}

	private void deserializeValue(T keySuffix, Send<Buffer> valueToReceive, SynchronousSink<U> sink) {
		try (var value = valueToReceive.receive()) {
			try {
				sink.next(valueSerializer.deserialize(value));
			} catch (IndexOutOfBoundsException ex) {
				var exMessage = ex.getMessage();
				if (exMessage != null && exMessage.contains("read 0 to 0, write 0 to ")) {
					var totalZeroBytesErrors = this.totalZeroBytesErrors.incrementAndGet();
					if (totalZeroBytesErrors < 512 || totalZeroBytesErrors % 10000 == 0) {
						try (var keySuffixBytes = serializeKeySuffixToKey(keySuffix)) {
							LOG.error("Unexpected zero-bytes value at " + dictionary.getDatabaseName()
									+ ":" + dictionary.getColumnName()
									+ ":" + LLUtils.toStringSafe(this.keyPrefix)
									+ ":" + keySuffix + "(" + LLUtils.toStringSafe(keySuffixBytes) + ") total=" + totalZeroBytesErrors);
						} catch (SerializationException e) {
							LOG.error("Unexpected zero-bytes value at " + dictionary.getDatabaseName()
									+ ":" + dictionary.getColumnName()
									+ ":" + LLUtils.toStringSafe(this.keyPrefix)
									+ ":" + keySuffix + "(?) total=" + totalZeroBytesErrors);
						}
					}
					sink.complete();
				} else {
					sink.error(ex);
				}
			} catch (Throwable ex) {
				sink.error(ex);
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
		if (keyPrefix != null) {
			keyBuf = keyPrefix.copy();
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
		if (keyPrefix != null && keyPrefix.readableBytes() > 0) {
			var result = LLUtils.compositeBuffer(dictionary.getAllocator(),
					LLUtils.copy(dictionary.getAllocator(), keyPrefix),
					suffixKey.send()
			);
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
	public Mono<Object2ObjectSortedMap<T, U>> get(@Nullable CompositeSnapshot snapshot, boolean existsAlmostCertainly) {
		return dictionary
				.getRange(resolveSnapshot(snapshot), rangeMono, false)
				.<Entry<T, U>>handle((entrySend, sink) -> {
					Entry<T, U> deserializedEntry;
					try {
						try (var entry = entrySend.receive()) {
							T key;
							try (var serializedKey = entry.getKey().receive()) {
								splitPrefix(serializedKey).close();
								suffixKeyLengthConsistency(serializedKey.readableBytes());
								key = deserializeSuffix(serializedKey);
							}
							U value;
							try (var valueBuf = entry.getValue().receive()) {
								value = valueSerializer.deserialize(valueBuf);
							}
							deserializedEntry = Map.entry(key, value);
						}
						sink.next(deserializedEntry);
					} catch (Throwable ex) {
						sink.error(ex);
					}
				})
				.collectMap(Entry::getKey, Entry::getValue, Object2ObjectLinkedOpenHashMap::new)
				.map(map -> (Object2ObjectSortedMap<T, U>) map)
				.filter(map -> !map.isEmpty());
	}

	@Override
	public Mono<Object2ObjectSortedMap<T, U>> setAndGetPrevious(Object2ObjectSortedMap<T, U> value) {
		return this
				.get(null, false)
				.concatWith(dictionary.setRange(rangeMono, Flux
						.fromIterable(Collections.unmodifiableMap(value).entrySet())
						.handle(this::serializeEntrySink)
				).then(Mono.empty()))
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
				new DatabaseMapSingle<>(dictionary, serializeKeySuffixToKey(keySuffix), valueSerializer, null));
	}

	@Override
	public Mono<Boolean> containsKey(@Nullable CompositeSnapshot snapshot, T keySuffix) {
		return dictionary
				.isRangeEmpty(resolveSnapshot(snapshot),
						Mono.fromCallable(() -> LLRange.singleUnsafe(serializeKeySuffixToKey(keySuffix)).send()),
						true
				)
				.map(empty -> !empty);
	}

	@Override
	public Mono<U> getValue(@Nullable CompositeSnapshot snapshot, T keySuffix, boolean existsAlmostCertainly) {
		return dictionary
				.get(resolveSnapshot(snapshot),
						Mono.fromCallable(() -> serializeKeySuffixToKey(keySuffix).send())
				)
				.handle((valueToReceive, sink) -> deserializeValue(keySuffix, valueToReceive, sink));
	}

	@Override
	public Mono<Void> putValue(T keySuffix, U value) {
		var keyMono = Mono.fromCallable(() -> serializeKeySuffixToKey(keySuffix).send()).single();
		var valueMono = Mono.fromCallable(() -> serializeValue(value).send()).single();
		return dictionary
				.put(keyMono, valueMono, LLDictionaryResultType.VOID)
				.doOnNext(Send::close)
				.then();
	}

	@Override
	public Mono<UpdateMode> getUpdateMode() {
		return dictionary.getUpdateMode();
	}

	@Override
	public Mono<U> updateValue(T keySuffix,
			UpdateReturnMode updateReturnMode,
			SerializationFunction<@Nullable U, @Nullable U> updater) {
		var keyMono = Mono.fromCallable(() -> serializeKeySuffixToKey(keySuffix).send());
		return dictionary
				.update(keyMono, getSerializedUpdater(updater), updateReturnMode)
				.handle((valueToReceive, sink) -> deserializeValue(keySuffix, valueToReceive, sink));
	}

	@Override
	public Mono<Delta<U>> updateValueAndGetDelta(T keySuffix,
			SerializationFunction<@Nullable U, @Nullable U> updater) {
		var keyMono = Mono.fromCallable(() -> serializeKeySuffixToKey(keySuffix).send());
		return  dictionary
				.updateAndGetDelta(keyMono, getSerializedUpdater(updater))
				.transform(mono -> LLUtils.mapLLDelta(mono, serializedToReceive -> {
					try (var serialized = serializedToReceive.receive()) {
						return valueSerializer.deserialize(serialized);
					}
				}));
	}

	public SerializationFunction<@Nullable Send<Buffer>, @Nullable Buffer> getSerializedUpdater(
			SerializationFunction<@Nullable U, @Nullable U> updater) {
		return oldSerialized -> {
			try (oldSerialized) {
				U result;
				if (oldSerialized == null) {
					result = updater.apply(null);
				} else {
					try (var oldSerializedReceived = oldSerialized.receive()) {
						result = updater.apply(valueSerializer.deserialize(oldSerializedReceived));
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

	public KVSerializationFunction<@NotNull T, @Nullable Send<Buffer>, @Nullable Buffer> getSerializedUpdater(
			KVSerializationFunction<@NotNull T, @Nullable U, @Nullable U> updater) {
		return (key, oldSerialized) -> {
			try (oldSerialized) {
				U result;
				if (oldSerialized == null) {
					result = updater.apply(key, null);
				} else {
					try (var oldSerializedReceived = oldSerialized.receive()) {
						result = updater.apply(key, valueSerializer.deserialize(oldSerializedReceived));
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
		var keyMono = Mono.fromCallable(() -> serializeKeySuffixToKey(keySuffix).send());
		var valueMono = Mono.fromCallable(() -> serializeValue(value).send());
		return dictionary
				.put(keyMono, valueMono, LLDictionaryResultType.PREVIOUS_VALUE)
				.handle((valueToReceive, sink) -> deserializeValue(keySuffix, valueToReceive, sink));
	}

	@Override
	public Mono<Boolean> putValueAndGetChanged(T keySuffix, U value) {
		var keyMono = Mono.fromCallable(() -> serializeKeySuffixToKey(keySuffix).send());
		var valueMono = Mono.fromCallable(() -> serializeValue(value).send());
		return dictionary
				.put(keyMono, valueMono, LLDictionaryResultType.PREVIOUS_VALUE)
				.handle((Send<Buffer> valueToReceive, SynchronousSink<U> sink) -> deserializeValue(keySuffix,
						valueToReceive,
						sink
				))
				.map(oldValue -> !Objects.equals(oldValue, value))
				.defaultIfEmpty(value != null);
	}

	@Override
	public Mono<Void> remove(T keySuffix) {
		var keyMono = Mono.fromCallable(() -> serializeKeySuffixToKey(keySuffix).send());
		return dictionary
				.remove(keyMono, LLDictionaryResultType.VOID)
				.doOnNext(Send::close)
				.then();
	}

	@Override
	public Mono<U> removeAndGetPrevious(T keySuffix) {
		var keyMono = Mono.fromCallable(() -> serializeKeySuffixToKey(keySuffix).send());
		return dictionary
				.remove(keyMono, LLDictionaryResultType.PREVIOUS_VALUE)
				.handle((valueToReceive, sink) -> deserializeValue(keySuffix, valueToReceive, sink));
	}

	@Override
	public Mono<Boolean> removeAndGetStatus(T keySuffix) {
		var keyMono = Mono.fromCallable(() -> serializeKeySuffixToKey(keySuffix).send());
		return dictionary
				.remove(keyMono, LLDictionaryResultType.PREVIOUS_VALUE_EXISTENCE)
				.map(LLUtils::responseToBoolean);
	}

	@Override
	public Flux<Optional<U>> getMulti(@Nullable CompositeSnapshot snapshot, Flux<T> keys, boolean existsAlmostCertainly) {
		var mappedKeys = keys
				.<Send<Buffer>>handle((keySuffix, sink) -> {
					try {
						sink.next(serializeKeySuffixToKey(keySuffix).send());
					} catch (Throwable ex) {
						sink.error(ex);
					}
				});
		return dictionary
				.getMulti(resolveSnapshot(snapshot), mappedKeys)
				.<Optional<U>>handle((valueBufOpt, sink) -> {
					try {
						Optional<U> valueOpt;
						if (valueBufOpt.isPresent()) {
							valueOpt = Optional.of(valueSerializer.deserialize(valueBufOpt.get()));
						} else {
							valueOpt = Optional.empty();
						}
						sink.next(valueOpt);
					} catch (Throwable ex) {
						sink.error(ex);
					} finally {
						valueBufOpt.ifPresent(Resource::close);
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

	private void serializeEntrySink(Entry<T,U> entry, SynchronousSink<Send<LLEntry>> sink) {
		try {
			sink.next(serializeEntry(entry.getKey(), entry.getValue()).send());
		} catch (Throwable e) {
			sink.error(e);
		}
	}

	@Override
	public Mono<Void> putMulti(Flux<Entry<T, U>> entries) {
		var serializedEntries = entries
				.<Send<LLEntry>>handle((entry, sink) -> {
					try {
						sink.next(serializeEntry(entry.getKey(), entry.getValue()).send());
					} catch (Throwable e) {
						sink.error(e);
					}
				});
		return dictionary.putMulti(serializedEntries);
	}

	@Override
	public Flux<Boolean> updateMulti(Flux<T> keys,
			KVSerializationFunction<T, @Nullable U, @Nullable U> updater) {
		var sharedKeys = keys.publish().refCount(2);
		var serializedKeys = sharedKeys.<Send<Buffer>>handle((key, sink) -> {
			try {
				Send<Buffer> serializedKey = serializeKeySuffixToKey(key).send();
				sink.next(serializedKey);
			} catch (Throwable ex) {
				sink.error(ex);
			}
		});
		var serializedUpdater = getSerializedUpdater(updater);
		return dictionary.updateMulti(sharedKeys, serializedKeys, serializedUpdater);
	}

	@Override
	public Flux<Entry<T, DatabaseStageEntry<U>>> getAllStages(@Nullable CompositeSnapshot snapshot) {
		return getAllStages(snapshot, rangeMono, false);
	}

	/**
	 * Get all stages
	 * @param key from/to the specified key, if not null
	 * @param reverse if true, the results will go backwards from the specified key (inclusive)
	 *                if false, the results will go forward from the specified key (inclusive)
	 */
	public Flux<Entry<T, DatabaseStageEntry<U>>> getAllStages(@Nullable CompositeSnapshot snapshot,
			@Nullable T key,
			boolean reverse) {
		if (key == null) {
			return getAllStages(snapshot);
		} else {
			Mono<Send<LLRange>> boundedRangeMono = rangeMono.flatMap(fullRangeSend -> Mono.fromCallable(() -> {
				try (var fullRange = fullRangeSend.receive()) {
					try (var keyWithoutExtBuf = keyPrefix == null ? alloc.allocate(keySuffixLength + keyExtLength)
							// todo: use a read-only copy
							: keyPrefix.copy()) {
						keyWithoutExtBuf.ensureWritable(keySuffixLength + keyExtLength);
						serializeSuffix(key, keyWithoutExtBuf);
						if (reverse) {
							return LLRange.of(fullRange.getMin(), keyWithoutExtBuf.send()).send();
						} else {
							return LLRange.of(keyWithoutExtBuf.send(), fullRange.getMax()).send();
						}
					}
				}
			}));
			return getAllStages(snapshot, boundedRangeMono, reverse);
		}
	}

	private Flux<Entry<T, DatabaseStageEntry<U>>> getAllStages(@Nullable CompositeSnapshot snapshot,
			Mono<Send<LLRange>> sliceRangeMono, boolean reverse) {
		return dictionary
				.getRangeKeys(resolveSnapshot(snapshot), sliceRangeMono, reverse)
				.handle((keyBufToReceive, sink) -> {
					var keyBuf = keyBufToReceive.receive();
					try {
						assert keyBuf.readableBytes() == keyPrefixLength + keySuffixLength + keyExtLength;
						// Remove prefix. Keep only the suffix and the ext
						splitPrefix(keyBuf).close();
						suffixKeyLengthConsistency(keyBuf.readableBytes());
						T keySuffix;
						try (var keyBufCopy = keyBuf.copy()) {
							keySuffix = deserializeSuffix(keyBufCopy);
						}
						var subStage = new DatabaseMapSingle<>(dictionary, toKey(keyBuf), valueSerializer, null);
						sink.next(Map.entry(keySuffix, subStage));
					} catch (Throwable ex) {
						keyBuf.close();
						sink.error(ex);
					}
				});
	}

	@Override
	public Flux<Entry<T, U>> getAllValues(@Nullable CompositeSnapshot snapshot) {
		return getAllValues(snapshot, rangeMono, false);
	}

	/**
	 * Get all values
	 * @param key from/to the specified key, if not null
	 * @param reverse if true, the results will go backwards from the specified key (inclusive)
	 *                if false, the results will go forward from the specified key (inclusive)
	 */
	public Flux<Entry<T, U>> getAllValues(@Nullable CompositeSnapshot snapshot, @Nullable T key, boolean reverse) {
		if (key == null) {
			return getAllValues(snapshot);
		} else {
			Mono<Send<LLRange>> boundedRangeMono = rangeMono.flatMap(fullRangeSend -> Mono.fromCallable(() -> {
				try (var fullRange = fullRangeSend.receive()) {
					try (var keyWithoutExtBuf = keyPrefix == null ? alloc.allocate(keySuffixLength + keyExtLength)
							// todo: use a read-only copy
							: keyPrefix.copy()) {
						keyWithoutExtBuf.ensureWritable(keySuffixLength + keyExtLength);
						serializeSuffix(key, keyWithoutExtBuf);
						if (reverse) {
							return LLRange.of(fullRange.getMin(), keyWithoutExtBuf.send()).send();
						} else {
							return LLRange.of(keyWithoutExtBuf.send(), fullRange.getMax()).send();
						}
					}
				}
			}));
			return getAllValues(snapshot, boundedRangeMono, reverse);
		}
	}

	private Flux<Entry<T, U>> getAllValues(@Nullable CompositeSnapshot snapshot,
			Mono<Send<LLRange>> sliceRangeMono,
			boolean reverse) {
		return dictionary
				.getRange(resolveSnapshot(snapshot), sliceRangeMono, reverse)
				.<Entry<T, U>>handle((serializedEntryToReceive, sink) -> {
					try {
						Entry<T, U> entry;
						try (var serializedEntry = serializedEntryToReceive.receive()) {
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
						sink.next(entry);
					} catch (Throwable e) {
						sink.error(e);
					}
				});
	}

	@Override
	public Flux<Entry<T, U>> setAllValuesAndGetPrevious(Flux<Entry<T, U>> entries) {
		return Flux.concat(
				this.getAllValues(null),
				dictionary.setRange(rangeMono, entries.handle(this::serializeEntrySink)).then(Mono.empty())
		);
	}

	@Override
	public Mono<Void> clear() {
		if (range.isAll()) {
			return dictionary.clear();
		} else if (range.isSingle()) {
			return dictionary
					.remove(Mono.fromCallable(range::getSingle), LLDictionaryResultType.VOID)
					.doOnNext(Send::close)
					.then();
		} else {
			return dictionary.setRange(rangeMono, Flux.empty());
		}
	}

}
