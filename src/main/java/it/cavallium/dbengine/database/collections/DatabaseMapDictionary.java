package it.cavallium.dbengine.database.collections;

import io.net5.buffer.api.Buffer;
import io.net5.buffer.api.Resource;
import io.net5.buffer.api.Send;
import io.net5.buffer.api.internal.ResourceSupport;
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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;
import reactor.util.function.Tuple2;

/**
 * Optimized implementation of "DatabaseMapDictionary with SubStageGetterSingle"
 */
public class DatabaseMapDictionary<T, U> extends DatabaseMapDictionaryDeep<T, U, DatabaseStageEntry<U>> {

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

	private void deserializeValue(Send<Buffer> valueToReceive, SynchronousSink<U> sink) {
		try (var value = valueToReceive.receive()) {
			sink.next(valueSerializer.deserialize(value));
		} catch (Throwable ex) {
			sink.error(ex);
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
				.getRange(resolveSnapshot(snapshot), rangeMono, existsAlmostCertainly)
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
				.singleOrEmpty()
				.transform(LLUtils::handleDiscard);
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
		return dictionary.isRangeEmpty(resolveSnapshot(snapshot), rangeMono);
	}

	@Override
	public Mono<DatabaseStageEntry<U>> at(@Nullable CompositeSnapshot snapshot, T keySuffix) {
		return Mono.fromCallable(() ->
				new DatabaseSingle<>(dictionary, serializeKeySuffixToKey(keySuffix), valueSerializer, null));
	}

	@Override
	public Mono<Boolean> containsKey(@Nullable CompositeSnapshot snapshot, T keySuffix) {
		return dictionary
				.isRangeEmpty(resolveSnapshot(snapshot),
						Mono.fromCallable(() -> LLRange.singleUnsafe(serializeKeySuffixToKey(keySuffix)).send())
				)
				.map(empty -> !empty);
	}

	@Override
	public Mono<U> getValue(@Nullable CompositeSnapshot snapshot, T keySuffix, boolean existsAlmostCertainly) {
		return dictionary
				.get(resolveSnapshot(snapshot),
						Mono.fromCallable(() -> serializeKeySuffixToKey(keySuffix).send()),
						existsAlmostCertainly
				)
				.handle(this::deserializeValue);
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
			boolean existsAlmostCertainly,
			SerializationFunction<@Nullable U, @Nullable U> updater) {
		var keyMono = Mono.fromCallable(() -> serializeKeySuffixToKey(keySuffix).send());
		return dictionary
				.update(keyMono, getSerializedUpdater(updater), updateReturnMode, existsAlmostCertainly)
				.handle(this::deserializeValue);
	}

	@Override
	public Mono<Delta<U>> updateValueAndGetDelta(T keySuffix,
			boolean existsAlmostCertainly,
			SerializationFunction<@Nullable U, @Nullable U> updater) {
		var keyMono = Mono.fromCallable(() -> serializeKeySuffixToKey(keySuffix).send());
		return  dictionary
				.updateAndGetDelta(keyMono, getSerializedUpdater(updater), existsAlmostCertainly)
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
		return dictionary.put(keyMono, valueMono, LLDictionaryResultType.PREVIOUS_VALUE).handle(this::deserializeValue);
	}

	@Override
	public Mono<Boolean> putValueAndGetChanged(T keySuffix, U value) {
		var keyMono = Mono.fromCallable(() -> serializeKeySuffixToKey(keySuffix).send());
		var valueMono = Mono.fromCallable(() -> serializeValue(value).send());
		return dictionary
				.put(keyMono, valueMono, LLDictionaryResultType.PREVIOUS_VALUE)
				.handle(this::deserializeValue)
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
		return dictionary.remove(keyMono, LLDictionaryResultType.PREVIOUS_VALUE).handle(this::deserializeValue);
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
				.getMulti(resolveSnapshot(snapshot), mappedKeys, existsAlmostCertainly)
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
				})
				.transform(LLUtils::handleDiscard);
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
				})
				.doOnDiscard(Send.class, Send::close)
				.doOnDiscard(Resource.class, Resource::close);
		return dictionary
				.putMulti(serializedEntries, false)
				.then()
				.doOnDiscard(Send.class, Send::close)
				.doOnDiscard(Resource.class, Resource::close)
				.doOnDiscard(LLEntry.class, ResourceSupport::close)
				.doOnDiscard(List.class, list -> {
					for (Object o : list) {
						if (o instanceof Send send) {
							send.close();
						} else if (o instanceof Buffer buf) {
							buf.close();
						}
					}
				});
	}

	@Override
	public Flux<Boolean> updateMulti(Flux<T> keys,
			KVSerializationFunction<T, @Nullable U, @Nullable U> updater) {
		var sharedKeys = keys.publish().refCount(2);
		var serializedKeys = sharedKeys
				.<Send<Buffer>>handle((key, sink) -> {
					try {
						Send<Buffer> serializedKey = serializeKeySuffixToKey(key).send();
						sink.next(serializedKey);
					} catch (Throwable ex) {
						sink.error(ex);
					}
				})
				.doOnDiscard(Tuple2.class, uncastedEntry -> {
					if (uncastedEntry.getT1() instanceof Buffer byteBuf) {
						byteBuf.close();
					}
					if (uncastedEntry.getT2() instanceof Buffer byteBuf) {
						byteBuf.close();
					}
				});
		var serializedUpdater = getSerializedUpdater(updater);
		return dictionary.updateMulti(sharedKeys, serializedKeys, serializedUpdater);
	}

	@Override
	public Flux<Entry<T, DatabaseStageEntry<U>>> getAllStages(@Nullable CompositeSnapshot snapshot) {
		return dictionary
				.getRangeKeys(resolveSnapshot(snapshot), rangeMono)
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
						var subStage = new DatabaseSingle<>(dictionary, toKey(keyBuf), valueSerializer, null);
						sink.next(Map.entry(keySuffix, subStage));
					} catch (Throwable ex) {
						keyBuf.close();
						sink.error(ex);
					}
				});
	}

	@Override
	public Flux<Entry<T, U>> getAllValues(@Nullable CompositeSnapshot snapshot) {
		return dictionary
				.getRange(resolveSnapshot(snapshot), rangeMono)
				.<Entry<T, U>>handle((serializedEntryToReceive, sink) -> {
					try {
						Entry<T, U> entry;
						try (var serializedEntry = serializedEntryToReceive.receive()) {
							var keyBuf = serializedEntry.getKeyUnsafe();
							assert keyBuf != null;
							assert keyBuf.readableBytes() == keyPrefixLength + keySuffixLength + keyExtLength;
							// Remove prefix. Keep only the suffix and the ext
							splitPrefix(keyBuf).close();
							suffixKeyLengthConsistency(keyBuf.readableBytes());
							T keySuffix = deserializeSuffix(keyBuf);

							assert serializedEntry.getValueUnsafe() != null;
							U value = valueSerializer.deserialize(serializedEntry.getValueUnsafe());
							entry = Map.entry(keySuffix, value);
						}
						sink.next(entry);
					} catch (Throwable e) {
						sink.error(e);
					}
				})
				.doOnDiscard(Entry.class, uncastedEntry -> {
					if (uncastedEntry.getKey() instanceof Buffer byteBuf) {
						byteBuf.close();
					}
					if (uncastedEntry.getValue() instanceof Buffer byteBuf) {
						byteBuf.close();
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
