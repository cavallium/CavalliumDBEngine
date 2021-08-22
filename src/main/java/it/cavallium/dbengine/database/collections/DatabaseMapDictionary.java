package it.cavallium.dbengine.database.collections;

import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCounted;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.Delta;
import it.cavallium.dbengine.database.ExtraKeyOperationResult;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.LLDictionaryResultType;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.UpdateMode;
import it.cavallium.dbengine.database.UpdateReturnMode;
import it.cavallium.dbengine.database.serialization.Serializer;
import it.cavallium.dbengine.database.serialization.SerializerFixedBinaryLength;
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
import reactor.util.function.Tuple2;
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

	@Override
	public Mono<Map<T, U>> get(@Nullable CompositeSnapshot snapshot, boolean existsAlmostCertainly) {
		return dictionary
				.getRange(resolveSnapshot(snapshot), rangeMono, existsAlmostCertainly)
				.collectMap(
						entry -> deserializeSuffix(stripPrefix(entry.getKey(), false)),
						entry -> valueSerializer.deserialize(entry.getValue()),
						HashMap::new)
				.filter(map -> !map.isEmpty());
	}

	@Override
	public Mono<Map<T, U>> setAndGetPrevious(Map<T, U> value) {
		return Mono
				.usingWhen(
						Mono.just(true),
						b -> get(null, false),
						b -> dictionary
								.setRange(rangeMono,
										Flux
												.fromIterable(Collections.unmodifiableMap(value).entrySet())
												.map(entry -> Map
														.entry(this.toKey(serializeSuffix(entry.getKey())),
																valueSerializer.serialize(entry.getValue())
														))
								)
				);
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
				.fromSupplier(() -> new DatabaseSingleMapped<>(
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
								.map(valueSerializer::deserialize),
						ReferenceCounted::release
				);
	}

	@Override
	public Mono<Void> putValue(T keySuffix, U value) {
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
																		LLDictionaryResultType.VOID)
																.doOnNext(ReferenceCounted::release),
														ReferenceCounted::release
												),
										ReferenceCounted::release
								),
						ReferenceCounted::release
				)
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
			Function<@Nullable U, @Nullable U> updater) {
		return Mono
				.using(
						() -> toKey(serializeSuffix(keySuffix)),
						keyBuf -> dictionary
								.update(LLUtils.lazyRetain(keyBuf), getSerializedUpdater(updater), updateReturnMode, existsAlmostCertainly)
								.map(valueSerializer::deserialize),
						ReferenceCounted::release
				);
	}

	@Override
	public Mono<Delta<U>> updateValueAndGetDelta(T keySuffix,
			boolean existsAlmostCertainly,
			Function<@Nullable U, @Nullable U> updater) {
		return Mono
				.using(
						() -> toKey(serializeSuffix(keySuffix)),
						keyBuf -> dictionary
								.updateAndGetDelta(LLUtils.lazyRetain(keyBuf), getSerializedUpdater(updater), existsAlmostCertainly)
								.transform(mono -> LLUtils.mapDelta(mono, valueSerializer::deserialize)),
						ReferenceCounted::release
				);
	}

	public Function<@Nullable ByteBuf, @Nullable ByteBuf> getSerializedUpdater(Function<@Nullable U, @Nullable U> updater) {
		return oldSerialized -> {
			try {
				var result = updater.apply(oldSerialized == null ? null : valueSerializer.deserialize(oldSerialized.retain()));
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

	public <X> BiFunction<@Nullable ByteBuf, X, @Nullable ByteBuf> getSerializedUpdater(BiFunction<@Nullable U, X, @Nullable U> updater) {
		return (oldSerialized, extra) -> {
			try {
				var result = updater.apply(oldSerialized == null ? null : valueSerializer.deserialize(oldSerialized.retain()), extra);
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
																.map(valueSerializer::deserialize),
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
																.map(valueSerializer::deserialize)
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
								.map(valueSerializer::deserialize),
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
		return dictionary
			.getMulti(resolveSnapshot(snapshot), keys.flatMap(keySuffix -> Mono.fromCallable(() -> {
				ByteBuf keySuffixBuf = serializeSuffix(keySuffix);
				try {
					return Tuples.of(keySuffix, toKey(keySuffixBuf.retain()));
				} finally {
					keySuffixBuf.release();
				}
			})), existsAlmostCertainly)
			.flatMapSequential(entry -> {
				entry.getT2().release();
				return Mono.fromCallable(() -> Map.entry(entry.getT1(), entry.getT3().map(valueSerializer::deserialize)));
			});
	}

	private Entry<ByteBuf, ByteBuf> serializeEntry(T key, U value) {
		ByteBuf serializedKey = toKey(serializeSuffix(key));
		try {
			ByteBuf serializedValue = valueSerializer.serialize(value);
			try {
				return Map.entry(serializedKey.retain(), serializedValue.retain());
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
				.flatMap(entry -> Mono
						.fromCallable(() -> serializeEntry(entry.getKey(), entry.getValue()))
						.doOnDiscard(Entry.class, uncastedEntry -> {
							if (uncastedEntry.getKey() instanceof ByteBuf byteBuf) {
								byteBuf.release();
							}
							if (uncastedEntry.getValue() instanceof ByteBuf byteBuf) {
								byteBuf.release();
							}
						})
				);
		return dictionary
				.putMulti(serializedEntries, false)
				.then();
	}

	@Override
	public <X> Flux<ExtraKeyOperationResult<T, X>> updateMulti(Flux<Tuple2<T, X>> entries,
			BiFunction<@Nullable U, X, @Nullable U> updater) {
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
				.map(result -> new ExtraKeyOperationResult<>(deserializeSuffix(result.key()),
						result.extra(),
						result.changed()
				));
	}

	@Override
	public Flux<Entry<T, DatabaseStageEntry<U>>> getAllStages(@Nullable CompositeSnapshot snapshot) {
		return dictionary
				.getRangeKeys(resolveSnapshot(snapshot), rangeMono)
				.map(key -> {
					ByteBuf keySuffixWithExt = stripPrefix(key.retain(), false);
					try {
						try {
							return Map.entry(deserializeSuffix(keySuffixWithExt.retainedSlice()),
									new DatabaseSingleMapped<>(new DatabaseSingle<>(dictionary,
											toKey(keySuffixWithExt.retainedSlice()),
											Serializer.noop()
									), valueSerializer)
							);
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
				.map(serializedEntry -> Map.entry(
						deserializeSuffix(stripPrefix(serializedEntry.getKey(), false)),
						valueSerializer.deserialize(serializedEntry.getValue())
				))
				.doOnDiscard(Entry.class, uncastedEntry -> {
					if (uncastedEntry.getKey() instanceof ByteBuf byteBuf) {
						byteBuf.release();
					}
					if (uncastedEntry.getValue() instanceof ByteBuf byteBuf) {
						byteBuf.release();
					}
				});
	}

	@Override
	public Flux<Entry<T, U>> setAllValuesAndGetPrevious(Flux<Entry<T, U>> entries) {
		return Flux
				.usingWhen(
						Mono.just(true),
						b -> getAllValues(null),
						b -> dictionary
								.setRange(rangeMono,
										entries.map(entry -> Map.entry(toKey(serializeSuffix(entry.getKey())),
												valueSerializer.serialize(entry.getValue())
										))
								)
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
