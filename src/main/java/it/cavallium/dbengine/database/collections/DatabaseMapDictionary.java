package it.cavallium.dbengine.database.collections;

import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCounted;
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
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuple3;
import reactor.util.function.Tuples;

/**
 * Optimized implementation of "DatabaseMapDictionary with SubStageGetterSingle"
 */
public class DatabaseMapDictionary<T, U> extends DatabaseMapDictionaryDeep<T, U, DatabaseStageEntry<U>> {

	private final Serializer<U, ByteBuf> valueSerializer;

	protected DatabaseMapDictionary(LLDictionary dictionary,
			ByteBuf prefixKey,
			SerializerFixedBinaryLength<T, ByteBuf> keySuffixSerializer,
			Serializer<U, ByteBuf> valueSerializer) {
		// Do not retain or release or use the prefixKey here
		super(dictionary, prefixKey, keySuffixSerializer, new SubStageGetterSingle<>(valueSerializer), 0);
		this.valueSerializer = valueSerializer;
	}

	public static <T, U> DatabaseMapDictionary<T, U> simple(LLDictionary dictionary,
			SerializerFixedBinaryLength<T, ByteBuf> keySerializer,
			Serializer<U, ByteBuf> valueSerializer) {
		return new DatabaseMapDictionary<>(dictionary, dictionary.getAllocator().buffer(0), keySerializer, valueSerializer);
	}

	public static <T, U> DatabaseMapDictionary<T, U> tail(LLDictionary dictionary,
			ByteBuf prefixKey,
			SerializerFixedBinaryLength<T, ByteBuf> keySuffixSerializer,
			Serializer<U, ByteBuf> valueSerializer) {
		return new DatabaseMapDictionary<>(dictionary, prefixKey, keySuffixSerializer, valueSerializer);
	}

	private ByteBuf toKey(ByteBuf suffixKey) {
		try {
			assert suffixKeyConsistency(suffixKey.readableBytes());
			return LLUtils.compositeBuffer(dictionary.getAllocator(), keyPrefix.retain(), suffixKey.retain());
		} finally {
			suffixKey.release();
		}
	}

	private void deserializeValue(ByteBuf value, SynchronousSink<U> sink) {
		try {
			sink.next(valueSerializer.deserialize(value));
		} catch (SerializationException ex) {
			sink.error(ex);
		}
	}

	@Override
	public Mono<Map<T, U>> get(@Nullable CompositeSnapshot snapshot, boolean existsAlmostCertainly) {
		return dictionary
				.getRange(resolveSnapshot(snapshot), rangeMono, existsAlmostCertainly)
				.<Entry<T, U>>handle((entry, sink) -> {
					try {
						var key = deserializeSuffix(stripPrefix(entry.getKey(), false));
						var value = valueSerializer.deserialize(entry.getValue());
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
								sink.next(new LLEntry(this.toKey(serializeSuffix(entry.getKey())),
										valueSerializer.serialize(entry.getValue())));
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
		return Mono
				.fromCallable(() -> new DatabaseSingleMapped<>(
						new DatabaseSingle<>(dictionary, toKey(serializeSuffix(keySuffix)), Serializer.noop())
						, valueSerializer)
				);
	}

	@Override
	public Mono<U> getValue(@Nullable CompositeSnapshot snapshot, T keySuffix, boolean existsAlmostCertainly) {
		return Mono
				.using(
						() -> toKey(serializeSuffix(keySuffix)),
						keyBuf -> dictionary
								.get(resolveSnapshot(snapshot), LLUtils.lazyRetain(keyBuf), existsAlmostCertainly)
								.handle(this::deserializeValue),
						ReferenceCounted::release
				);
	}

	@Override
	public Mono<Void> putValue(T keySuffix, U value) {
		return Mono.using(() -> serializeSuffix(keySuffix),
				keySuffixBuf -> Mono.using(() -> toKey(keySuffixBuf.retain()),
						keyBuf -> Mono.using(() -> valueSerializer.serialize(value),
								valueBuf -> dictionary
										.put(LLUtils.lazyRetain(keyBuf), LLUtils.lazyRetain(valueBuf), LLDictionaryResultType.VOID)
										.doOnNext(ReferenceCounted::release),
								ReferenceCounted::release
						),
						ReferenceCounted::release
				),
				ReferenceCounted::release
		).then();
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
		return Mono
				.using(
						() -> toKey(serializeSuffix(keySuffix)),
						keyBuf -> dictionary
								.update(LLUtils.lazyRetain(keyBuf), getSerializedUpdater(updater), updateReturnMode, existsAlmostCertainly)
								.handle(this::deserializeValue),
						ReferenceCounted::release
				);
	}

	@Override
	public Mono<Delta<U>> updateValueAndGetDelta(T keySuffix,
			boolean existsAlmostCertainly,
			SerializationFunction<@Nullable U, @Nullable U> updater) {
		return Mono
				.using(
						() -> toKey(serializeSuffix(keySuffix)),
						keyBuf -> dictionary
								.updateAndGetDelta(LLUtils.lazyRetain(keyBuf), getSerializedUpdater(updater), existsAlmostCertainly)
								.transform(mono -> LLUtils.mapDelta(mono, valueSerializer::deserialize)),
						ReferenceCounted::release
				);
	}

	public SerializationFunction<@Nullable ByteBuf, @Nullable ByteBuf> getSerializedUpdater(SerializationFunction<@Nullable U, @Nullable U> updater) {
		return oldSerialized -> {
			try {
				U result;
				if (oldSerialized == null) {
					result = updater.apply(null);
				} else {
					result = updater.apply(valueSerializer.deserialize(oldSerialized.retain()));
				}
				if (result == null) {
					return null;
				} else {
					return valueSerializer.serialize(result);
				}
			} finally {
				if (oldSerialized != null) {
					oldSerialized.release();
				}
			}
		};
	}

	public <X> BiSerializationFunction<@Nullable ByteBuf, X, @Nullable ByteBuf> getSerializedUpdater(
			BiSerializationFunction<@Nullable U, X, @Nullable U> updater) {
		return (oldSerialized, extra) -> {
			try {
				U result;
				if (oldSerialized == null) {
					result = updater.apply(null, extra);
				} else {
					result = updater.apply(valueSerializer.deserialize(oldSerialized.retain()), extra);
				}
				if (result == null) {
					return null;
				} else {
					return valueSerializer.serialize(result);
				}
			} finally {
				if (oldSerialized != null) {
					oldSerialized.release();
				}
			}
		};
	}

	@Override
	public Mono<U> putValueAndGetPrevious(T keySuffix, U value) {
		return Mono
				.using(
						() -> serializeSuffix(keySuffix),
						keySuffixBuf -> Mono
								.using(
										() -> toKey(keySuffixBuf.retain()),
										keyBuf -> Mono
												.using(() -> valueSerializer.serialize(value),
														valueBuf -> dictionary
																.put(LLUtils.lazyRetain(keyBuf),
																		LLUtils.lazyRetain(valueBuf),
																		LLDictionaryResultType.PREVIOUS_VALUE)
																.handle(this::deserializeValue),
														ReferenceCounted::release
												),
										ReferenceCounted::release
								),
						ReferenceCounted::release
				);
	}

	@Override
	public Mono<Boolean> putValueAndGetChanged(T keySuffix, U value) {
		return Mono
				.using(
						() -> serializeSuffix(keySuffix),
						keySuffixBuf -> Mono
								.using(
										() -> toKey(keySuffixBuf.retain()),
										keyBuf -> Mono
												.using(() -> valueSerializer.serialize(value),
														valueBuf -> dictionary
																.put(LLUtils.lazyRetain(keyBuf),
																		LLUtils.lazyRetain(valueBuf),
																		LLDictionaryResultType.PREVIOUS_VALUE
																)
																.handle(this::deserializeValue)
																.map(oldValue -> !Objects.equals(oldValue, value))
																.defaultIfEmpty(value != null),
														ReferenceCounted::release
												),
										ReferenceCounted::release
								),
						ReferenceCounted::release
				);
	}

	@Override
	public Mono<Void> remove(T keySuffix) {
		return Mono
				.using(
						() -> toKey(serializeSuffix(keySuffix)),
						keyBuf -> dictionary
								.remove(LLUtils.lazyRetain(keyBuf), LLDictionaryResultType.VOID)
								.doOnNext(ReferenceCounted::release)
								.then(),
						ReferenceCounted::release
				);
	}

	@Override
	public Mono<U> removeAndGetPrevious(T keySuffix) {
		return Mono
				.using(
						() -> toKey(serializeSuffix(keySuffix)),
						keyBuf -> dictionary
								.remove(LLUtils.lazyRetain(keyBuf), LLDictionaryResultType.PREVIOUS_VALUE)
								.handle(this::deserializeValue),
						ReferenceCounted::release
				);
	}

	@Override
	public Mono<Boolean> removeAndGetStatus(T keySuffix) {
		return Mono
				.using(
						() -> toKey(serializeSuffix(keySuffix)),
						keyBuf -> dictionary
								.remove(LLUtils.lazyRetain(keyBuf), LLDictionaryResultType.PREVIOUS_VALUE_EXISTENCE)
								.map(LLUtils::responseToBoolean),
						ReferenceCounted::release
				);
	}

	@Override
	public Flux<Entry<T, Optional<U>>> getMulti(@Nullable CompositeSnapshot snapshot, Flux<T> keys, boolean existsAlmostCertainly) {
		return dictionary.getMulti(resolveSnapshot(snapshot), keys.flatMap(keySuffix -> Mono.fromCallable(() -> {
			ByteBuf keySuffixBuf = serializeSuffix(keySuffix);
			try {
				var key = toKey(keySuffixBuf.retain());
				try {
					return Tuples.of(keySuffix, key.retain());
				} finally {
					key.release();
				}
			} finally {
				keySuffixBuf.release();
			}
		})), existsAlmostCertainly).flatMapSequential(entry -> {
			entry.getT2().release();
			return Mono.fromCallable(() -> {
				Optional<U> valueOpt;
				if (entry.getT3().isPresent()) {
					var buf = entry.getT3().get();
					try {
						valueOpt = Optional.of(valueSerializer.deserialize(buf.retain()));
					} finally {
						buf.release();
					}
				} else {
					valueOpt = Optional.empty();
				}
				return Map.entry(entry.getT1(), valueOpt);
			});
		}).transform(LLUtils::handleDiscard);
	}

	private LLEntry serializeEntry(T key, U value) throws SerializationException {
		ByteBuf serializedKey = toKey(serializeSuffix(key));
		try {
			ByteBuf serializedValue = valueSerializer.serialize(value);
			try {
				return new LLEntry(serializedKey.retain(), serializedValue.retain());
			} finally {
				serializedValue.release();
			}
		} finally {
			serializedKey.release();
		}
	}

	@Override
	public Mono<Void> putMulti(Flux<Entry<T, U>> entries) {
		var serializedEntries = entries
				.<LLEntry>handle((entry, sink) -> {
					try {
						sink.next(serializeEntry(entry.getKey(), entry.getValue()));
					} catch (SerializationException e) {
						sink.error(e);
					}
				});
		return dictionary
				.putMulti(serializedEntries, false)
				.then()
				.doOnDiscard(LLEntry.class, entry -> {
					if (!entry.isReleased()) {
						entry.release();
					}
				});
	}

	@Override
	public <X> Flux<ExtraKeyOperationResult<T, X>> updateMulti(Flux<Tuple2<T, X>> entries,
			BiSerializationFunction<@Nullable U, X, @Nullable U> updater) {
		Flux<Tuple2<ByteBuf, X>> serializedEntries = entries
				.flatMap(entry -> Mono
						.fromCallable(() -> Tuples.of(serializeSuffix(entry.getT1()), entry.getT2()))
				)
				.doOnDiscard(Tuple2.class, uncastedEntry -> {
					if (uncastedEntry.getT1() instanceof ByteBuf byteBuf) {
						byteBuf.release();
					}
					if (uncastedEntry.getT2() instanceof ByteBuf byteBuf) {
						byteBuf.release();
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
				.handle((key, sink) -> {
					try {
						ByteBuf keySuffixWithExt = stripPrefix(key.retain(), false);
						try {
							sink.next(Map.entry(deserializeSuffix(keySuffixWithExt.retainedSlice()),
									new DatabaseSingleMapped<>(new DatabaseSingle<>(dictionary,
											toKey(keySuffixWithExt.retainedSlice()),
											Serializer.noop()
									), valueSerializer)
							));
						} catch (SerializationException ex) {
							sink.error(ex);
						} finally {
							keySuffixWithExt.release();
						}
					} finally {
						key.release();
					}
				});
	}

	@Override
	public Flux<Entry<T, U>> getAllValues(@Nullable CompositeSnapshot snapshot) {
		return dictionary
				.getRange(resolveSnapshot(snapshot), rangeMono)
				.<Entry<T, U>>handle((serializedEntry, sink) -> {
					ByteBuf key = serializedEntry.getKey();
					ByteBuf value = serializedEntry.getValue();
					try {
						ByteBuf keySuffix = stripPrefix(key.retain(), false);
						try {
							sink.next(Map.entry(deserializeSuffix(keySuffix.retain()),
									valueSerializer.deserialize(value.retain())));
						} finally {
							keySuffix.release();
						}
					} catch (SerializationException e) {
						sink.error(e);
					} finally {
						key.release();
						value.release();
					}
				})
				.doOnDiscard(Entry.class, uncastedEntry -> {
					if (uncastedEntry.getKey() instanceof ByteBuf byteBuf) {
						if (byteBuf.refCnt() > 0) {
							byteBuf.release();
						}
					}
					if (uncastedEntry.getValue() instanceof ByteBuf byteBuf) {
						if (byteBuf.refCnt() > 0) {
							byteBuf.release();
						}
					}
				});
	}

	@Override
	public Flux<Entry<T, U>> setAllValuesAndGetPrevious(Flux<Entry<T, U>> entries) {
		return Flux.concat(
				this.getAllValues(null),
				dictionary.setRange(rangeMono, entries.handle((entry, sink) -> {
					try {
						ByteBuf serializedKey = toKey(serializeSuffix(entry.getKey()));
						try {
							ByteBuf serializedValue = valueSerializer.serialize(entry.getValue());
							try {
								sink.next(new LLEntry(serializedKey.retain(), serializedValue.retain()));
							} finally {
								serializedValue.release();
							}
						} finally {
							serializedKey.release();
						}
					} catch (SerializationException e) {
						sink.error(e);
					}
				})).then(Mono.empty())
		);
	}

	@Override
	public Mono<Void> clear() {
		return Mono.defer(() -> {
			if (range.isAll()) {
				return dictionary.clear();
			} else if (range.isSingle()) {
				return dictionary
						.remove(LLUtils.lazyRetain(range.getSingle()), LLDictionaryResultType.VOID)
						.doOnNext(ReferenceCounted::release)
						.then();
			} else {
				return dictionary.setRange(rangeMono, Flux.empty());
			}
		});
	}

}
