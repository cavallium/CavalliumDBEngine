package it.cavallium.dbengine.database.collections;

import io.net5.buffer.api.Buffer;
import io.net5.buffer.api.Drop;
import io.net5.buffer.api.Send;
import io.net5.buffer.api.internal.ResourceSupport;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.Delta;
import it.cavallium.dbengine.database.ExtraKeyOperationResult;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.LLDictionaryResultType;
import it.cavallium.dbengine.database.LLEntry;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.UpdateMode;
import it.cavallium.dbengine.database.UpdateReturnMode;
import it.cavallium.dbengine.database.serialization.BiSerializationFunction;
import it.cavallium.dbengine.database.serialization.SerializationException;
import it.cavallium.dbengine.database.serialization.SerializationFunction;
import it.cavallium.dbengine.database.serialization.Serializer;
import it.cavallium.dbengine.database.serialization.SerializerFixedBinaryLength;
import java.util.Collections;
import java.util.HashMap;
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
import reactor.util.function.Tuples;

/**
 * Optimized implementation of "DatabaseMapDictionary with SubStageGetterSingle"
 */
public class DatabaseMapDictionary<T, U> extends DatabaseMapDictionaryDeep<T, U, DatabaseStageEntry<U>> {

	private final Serializer<U> valueSerializer;

	protected DatabaseMapDictionary(LLDictionary dictionary,
			@NotNull Send<Buffer> prefixKey,
			SerializerFixedBinaryLength<T> keySuffixSerializer,
			Serializer<U> valueSerializer,
			Drop<DatabaseMapDictionaryDeep<T, U, DatabaseStageEntry<U>>> drop) {
		// Do not retain or release or use the prefixKey here
		super(dictionary, prefixKey, keySuffixSerializer, new SubStageGetterSingle<>(valueSerializer), 0, drop);
		this.valueSerializer = valueSerializer;
	}

	public static <T, U> DatabaseMapDictionary<T, U> simple(LLDictionary dictionary,
			SerializerFixedBinaryLength<T> keySerializer,
			Serializer<U> valueSerializer,
			Drop<DatabaseMapDictionaryDeep<T, U, DatabaseStageEntry<U>>> drop) {
		return new DatabaseMapDictionary<>(dictionary, LLUtils.empty(dictionary.getAllocator()), keySerializer,
				valueSerializer, drop);
	}

	public static <T, U> DatabaseMapDictionary<T, U> tail(LLDictionary dictionary,
			Send<Buffer> prefixKey,
			SerializerFixedBinaryLength<T> keySuffixSerializer,
			Serializer<U> valueSerializer,
			Drop<DatabaseMapDictionaryDeep<T, U, DatabaseStageEntry<U>>> drop) {
		return new DatabaseMapDictionary<>(dictionary, prefixKey, keySuffixSerializer, valueSerializer, drop);
	}

	private Send<Buffer> toKey(Send<Buffer> suffixKeyToSend) {
		try (var suffixKey = suffixKeyToSend.receive()) {
			assert suffixKeyConsistency(suffixKey.readableBytes());
			if (keyPrefix.readableBytes() > 0) {
				try (var result = LLUtils.compositeBuffer(dictionary.getAllocator(),
						LLUtils.copy(dictionary.getAllocator(), keyPrefix),
						suffixKey.send()
				)) {
					assert result.readableBytes() == keyPrefixLength + keySuffixLength + keyExtLength;
					return result.send();
				}
			} else {
				assert suffixKey.readableBytes() == keyPrefixLength + keySuffixLength + keyExtLength;
				return suffixKey.send();
			}
		}
	}

	private void deserializeValue(Send<Buffer> value, SynchronousSink<U> sink) {
		try {
			sink.next(valueSerializer.deserialize(value).deserializedData());
		} catch (SerializationException ex) {
			sink.error(ex);
		}
	}

	@Override
	public Mono<Map<T, U>> get(@Nullable CompositeSnapshot snapshot, boolean existsAlmostCertainly) {
		return dictionary
				.getRange(resolveSnapshot(snapshot), rangeMono, existsAlmostCertainly)
				.<Entry<T, U>>handle((entrySend, sink) -> {
					try (var entry = entrySend.receive()) {
						T key;
						try (var serializedKey = entry.getKey().receive()) {
							removePrefix(serializedKey);
							suffixKeyConsistency(serializedKey.readableBytes());
							key = deserializeSuffix(serializedKey.send());
						}
						var value = valueSerializer.deserialize(entry.getValue()).deserializedData();
						sink.next(Map.entry(key, value));
					} catch (SerializationException ex) {
						sink.error(ex);
					}
				})
				.collectMap(Entry::getKey, Entry::getValue, HashMap::new)
				.filter(map -> !map.isEmpty());
	}

	@Override
	public Mono<Map<T, U>> setAndGetPrevious(Map<T, U> value) {
		return this
				.get(null, false)
				.concatWith(dictionary.setRange(rangeMono, Flux
						.fromIterable(Collections.unmodifiableMap(value).entrySet())
						.handle((entry, sink) -> {
							try {
								sink.next(LLEntry.of(this.toKey(serializeSuffix(entry.getKey())),
										valueSerializer.serialize(entry.getValue())).send());
							} catch (SerializationException e) {
								sink.error(e);
							}
						})
				).then(Mono.empty()))
				.singleOrEmpty()
				.transform(LLUtils::handleDiscard);
	}

	@Override
	public Mono<Map<T, U>> clearAndGetPrevious() {
		return this
				.setAndGetPrevious(Map.of());
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
				new DatabaseSingle<>(dictionary, toKey(serializeSuffix(keySuffix)), valueSerializer, d -> {}));
	}

	@Override
	public Mono<U> getValue(@Nullable CompositeSnapshot snapshot, T keySuffix, boolean existsAlmostCertainly) {
		return dictionary
				.get(resolveSnapshot(snapshot),
						Mono.fromCallable(() -> toKey(serializeSuffix(keySuffix))),
						existsAlmostCertainly
				)
				.handle(this::deserializeValue);
	}

	@Override
	public Mono<Void> putValue(T keySuffix, U value) {
		var keyMono = Mono.fromCallable(() -> toKey(serializeSuffix(keySuffix))).single();
		var valueMono = Mono.fromCallable(() -> valueSerializer.serialize(value)).single();
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
		var keyMono = Mono.fromCallable(() -> toKey(serializeSuffix(keySuffix)));
		return dictionary
				.update(keyMono, getSerializedUpdater(updater), updateReturnMode, existsAlmostCertainly)
				.handle(this::deserializeValue);
	}

	@Override
	public Mono<Delta<U>> updateValueAndGetDelta(T keySuffix,
			boolean existsAlmostCertainly,
			SerializationFunction<@Nullable U, @Nullable U> updater) {
		var keyMono = Mono.fromCallable(() -> toKey(serializeSuffix(keySuffix)));
		return  dictionary
				.updateAndGetDelta(keyMono, getSerializedUpdater(updater), existsAlmostCertainly)
				.transform(mono -> LLUtils.mapLLDelta(mono,
						serialized -> valueSerializer.deserialize(serialized).deserializedData()
				));
	}

	public SerializationFunction<@Nullable Send<Buffer>, @Nullable Send<Buffer>> getSerializedUpdater(
			SerializationFunction<@Nullable U, @Nullable U> updater) {
		return oldSerialized -> {
			try (oldSerialized) {
				U result;
				if (oldSerialized == null) {
					result = updater.apply(null);
				} else {
					result = updater.apply(valueSerializer.deserialize(oldSerialized).deserializedData());
				}
				if (result == null) {
					return null;
				} else {
					return valueSerializer.serialize(result);
				}
			}
		};
	}

	public <X> BiSerializationFunction<@Nullable Send<Buffer>, X, @Nullable Send<Buffer>> getSerializedUpdater(
			BiSerializationFunction<@Nullable U, X, @Nullable U> updater) {
		return (oldSerialized, extra) -> {
			try (oldSerialized) {
				U result;
				if (oldSerialized == null) {
					result = updater.apply(null, extra);
				} else {
					result = updater.apply(valueSerializer.deserialize(oldSerialized).deserializedData(), extra);
				}
				if (result == null) {
					return null;
				} else {
					return valueSerializer.serialize(result);
				}
			}
		};
	}

	@Override
	public Mono<U> putValueAndGetPrevious(T keySuffix, U value) {
		var keyMono = Mono.fromCallable(() -> toKey(serializeSuffix(keySuffix)));
		var valueMono = Mono.fromCallable(() -> valueSerializer.serialize(value));
		return dictionary
				.put(keyMono,
						valueMono,
						LLDictionaryResultType.PREVIOUS_VALUE)
				.handle(this::deserializeValue);
	}

	@Override
	public Mono<Boolean> putValueAndGetChanged(T keySuffix, U value) {
		var keyMono = Mono.fromCallable(() -> toKey(serializeSuffix(keySuffix)));
		var valueMono = Mono.fromCallable(() -> valueSerializer.serialize(value));
		return dictionary
				.put(keyMono, valueMono, LLDictionaryResultType.PREVIOUS_VALUE)
				.handle(this::deserializeValue)
				.map(oldValue -> !Objects.equals(oldValue, value))
				.defaultIfEmpty(value != null);
	}

	@Override
	public Mono<Void> remove(T keySuffix) {
		var keyMono = Mono.fromCallable(() -> toKey(serializeSuffix(keySuffix)));
		return dictionary
				.remove(keyMono, LLDictionaryResultType.VOID)
				.doOnNext(Send::close)
				.then();
	}

	@Override
	public Mono<U> removeAndGetPrevious(T keySuffix) {
		var keyMono = Mono.fromCallable(() -> toKey(serializeSuffix(keySuffix)));
		return dictionary
				.remove(keyMono, LLDictionaryResultType.PREVIOUS_VALUE)
				.handle(this::deserializeValue);
	}

	@Override
	public Mono<Boolean> removeAndGetStatus(T keySuffix) {
		var keyMono = Mono.fromCallable(() -> toKey(serializeSuffix(keySuffix)));
		return dictionary
				.remove(keyMono, LLDictionaryResultType.PREVIOUS_VALUE_EXISTENCE)
				.map(LLUtils::responseToBoolean);
	}

	@Override
	public Flux<Entry<T, Optional<U>>> getMulti(@Nullable CompositeSnapshot snapshot, Flux<T> keys, boolean existsAlmostCertainly) {
		var mappedKeys = keys
				.<Tuple2<T, Send<Buffer>>>handle((keySuffix, sink) -> {
					try {
						sink.next(Tuples.of(keySuffix, toKey(serializeSuffix(keySuffix))));
					} catch (SerializationException ex) {
						sink.error(ex);
					}
				});
		return dictionary
				.getMulti(resolveSnapshot(snapshot), mappedKeys, existsAlmostCertainly)
				.<Entry<T, Optional<U>>>handle((entry, sink) -> {
					try {
						Optional<U> valueOpt;
						if (entry.getT3().isPresent()) {
							try (var buf = entry.getT3().get()) {
								valueOpt = Optional.of(valueSerializer.deserialize(buf).deserializedData());
							}
						} else {
							valueOpt = Optional.empty();
						}
						sink.next(Map.entry(entry.getT1(), valueOpt));
					} catch (SerializationException ex) {
						sink.error(ex);
					} finally {
						entry.getT2().close();
						entry.getT3().ifPresent(Send::close);
					}
				})
				.transform(LLUtils::handleDiscard);
	}

	private Send<LLEntry> serializeEntry(T key, U value) throws SerializationException {
		try (var serializedKey = toKey(serializeSuffix(key))) {
			var serializedValueToReceive = valueSerializer.serialize(value);
			try (var serializedValue = serializedValueToReceive.receive()) {
				return LLEntry.of(serializedKey, serializedValue.send()).send();
			}
		}
	}

	@Override
	public Mono<Void> putMulti(Flux<Entry<T, U>> entries) {
		var serializedEntries = entries
				.<Send<LLEntry>>handle((entry, sink) -> {
					try {
						sink.next(serializeEntry(entry.getKey(), entry.getValue()));
					} catch (SerializationException e) {
						sink.error(e);
					}
				})
				.doOnDiscard(Send.class, Send::close);
		return dictionary
				.putMulti(serializedEntries, false)
				.then()
				.doOnDiscard(Send.class, Send::close)
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
	public <X> Flux<ExtraKeyOperationResult<T, X>> updateMulti(Flux<Tuple2<T, X>> entries,
			BiSerializationFunction<@Nullable U, X, @Nullable U> updater) {
		var serializedEntries = entries
				.<Tuple2<Send<Buffer>, X>>handle((entry, sink) -> {
					try {
						sink.next(Tuples.of(serializeSuffix(entry.getT1()), entry.getT2()));
					} catch (SerializationException ex) {
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
		return dictionary.updateMulti(serializedEntries, serializedUpdater)
				.handle((result, sink) -> {
					try {
						sink.next(new ExtraKeyOperationResult<>(deserializeSuffix(result.key()),
								result.extra(),
								result.changed()
						));
					} catch (SerializationException ex) {
						sink.error(ex);
					}
				});
	}

	@Override
	public Flux<Entry<T, DatabaseStageEntry<U>>> getAllStages(@Nullable CompositeSnapshot snapshot) {
		return dictionary
				.getRangeKeys(resolveSnapshot(snapshot), rangeMono)
				.handle((keyBufToReceive, sink) -> {
					try (var keyBuf = keyBufToReceive.receive()) {
						assert keyBuf.readableBytes() == keyPrefixLength + keySuffixLength + keyExtLength;
						// Remove prefix. Keep only the suffix and the ext
						removePrefix(keyBuf);
						suffixKeyConsistency(keyBuf.readableBytes());
						sink.next(Map.entry(deserializeSuffix(keyBuf.copy().send()),
								new DatabaseSingle<>(dictionary, toKey(keyBuf.send()), valueSerializer, d -> {})
						));
					} catch (SerializationException ex) {
						sink.error(ex);
					}
				});
	}

	@Override
	public Flux<Entry<T, U>> getAllValues(@Nullable CompositeSnapshot snapshot) {
		return dictionary
				.getRange(resolveSnapshot(snapshot), rangeMono)
				.<Entry<T, U>>handle((serializedEntryToReceive, sink) -> {
					try (var serializedEntry = serializedEntryToReceive.receive()) {
						try (var keyBuf = serializedEntry.getKey().receive()) {
							assert keyBuf.readableBytes() == keyPrefixLength + keySuffixLength + keyExtLength;
							// Remove prefix. Keep only the suffix and the ext
							removePrefix(keyBuf);
							suffixKeyConsistency(keyBuf.readableBytes());
							sink.next(Map.entry(deserializeSuffix(keyBuf.send()),
									valueSerializer.deserialize(serializedEntry.getValue()).deserializedData()));
						}
					} catch (SerializationException e) {
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
				dictionary.setRange(rangeMono, entries.handle((entry, sink) -> {
					try {
						sink.next(LLEntry.of(toKey(serializeSuffix(entry.getKey())),
								valueSerializer.serialize(entry.getValue())).send());
					} catch (SerializationException e) {
						sink.error(e);
					}
				})).then(Mono.empty())
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
