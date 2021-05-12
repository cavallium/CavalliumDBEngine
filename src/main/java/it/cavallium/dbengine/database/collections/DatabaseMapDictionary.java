package it.cavallium.dbengine.database.collections;

import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCounted;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.Delta;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.LLDictionaryResultType;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.UpdateMode;
import it.cavallium.dbengine.database.UpdateReturnMode;
import it.cavallium.dbengine.database.serialization.Serializer;
import it.cavallium.dbengine.database.serialization.SerializerFixedBinaryLength;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.function.Function;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

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
		prefixKey = null;
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
		return Flux
				.defer(() -> dictionary.getRange(resolveSnapshot(snapshot), range.retain(), existsAlmostCertainly))
				.collectMap(
						entry -> deserializeSuffix(stripPrefix(entry.getKey(), false)),
						entry -> deserialize(entry.getValue()),
						HashMap::new)
				.filter(map -> !map.isEmpty())
				.doFirst(() -> range.retain())
				.doFinally(s -> range.release());
	}

	@Override
	public Mono<Map<T, U>> setAndGetPrevious(Map<T, U> value) {
		return Mono
				.usingWhen(
						Mono.just(true),
						b -> get(null, false),
						b -> dictionary
								.setRange(range.retain(),
										Flux
												.fromIterable(value.entrySet())
												.map(entry -> Map
														.entry(this.toKey(serializeSuffix(entry.getKey())), serialize(entry.getValue()))
												)
								)
				)
				.doFirst(() -> range.retain())
				.doFinally(s -> range.release());
	}

	@Override
	public Mono<Map<T, U>> clearAndGetPrevious() {
		return this
				.setAndGetPrevious(Map.of());
	}

	@Override
	public Mono<Long> leavesCount(@Nullable CompositeSnapshot snapshot, boolean fast) {
		return Mono.defer(() -> dictionary.sizeRange(resolveSnapshot(snapshot), range.retain(), fast))
				.doFirst(() -> range.retain())
				.doFinally(s -> range.release());
	}

	@Override
	public Mono<Boolean> isEmpty(@Nullable CompositeSnapshot snapshot) {
		return Mono.defer(() -> dictionary.isRangeEmpty(resolveSnapshot(snapshot), range.retain()))
				.doFirst(() -> range.retain())
				.doFinally(s -> range.release());
	}

	@Override
	public Mono<DatabaseStageEntry<U>> at(@Nullable CompositeSnapshot snapshot, T keySuffix) {
		return Mono
				.using(
						() -> toKey(serializeSuffix(keySuffix)),
						keyBuf -> Mono
								.fromSupplier(() -> new DatabaseSingle<>(dictionary, keyBuf.retain(), Serializer.noop()))
								.<DatabaseStageEntry<U>>map(entry -> new DatabaseSingleMapped<>(entry, valueSerializer)),
						ReferenceCounted::release
				);
	}

	@Override
	public Mono<U> getValue(@Nullable CompositeSnapshot snapshot, T keySuffix, boolean existsAlmostCertainly) {
		return Mono
				.using(
						() -> toKey(serializeSuffix(keySuffix)),
						keyBuf -> dictionary
								.get(resolveSnapshot(snapshot), keyBuf.retain(), existsAlmostCertainly)
								.map(this::deserialize),
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
												.using(
														() -> serialize(value),
														valueBuf -> dictionary
																.put(keyBuf.retain(), valueBuf.retain(), LLDictionaryResultType.VOID)
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
								.update(keyBuf.retain(), oldSerialized -> {
									try {
										var result = updater.apply(oldSerialized == null ? null : this.deserialize(oldSerialized.retain()));
										if (result == null) {
											return null;
										} else {
											return this.serialize(result);
										}
									} finally {
										if (oldSerialized != null) {
											oldSerialized.release();
										}
									}
								}, updateReturnMode, existsAlmostCertainly)
								.map(this::deserialize),
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
								.updateAndGetDelta(keyBuf.retain(), oldSerialized -> {
									try {
										var result = updater.apply(oldSerialized == null ? null : this.deserialize(oldSerialized.retain()));
										if (result == null) {
											return null;
										} else {
											return this.serialize(result);
										}
									} finally {
										if (oldSerialized != null) {
											oldSerialized.release();
										}
									}
								}, existsAlmostCertainly)
								.transform(mono -> LLUtils.mapDelta(mono, this::deserialize)),
						ReferenceCounted::release
				);
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
												.using(
														() -> serialize(value),
														valueBuf -> dictionary
																.put(keyBuf.retain(), valueBuf.retain(), LLDictionaryResultType.PREVIOUS_VALUE)
																.map(this::deserialize),
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
												.using(
														() -> serialize(value),
														valueBuf -> dictionary
																.put(keyBuf.retain(), valueBuf.retain(), LLDictionaryResultType.PREVIOUS_VALUE)
																.map(this::deserialize)
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
								.remove(keyBuf.retain(), LLDictionaryResultType.VOID)
								.doOnNext(ReferenceCounted::release)
								.then(),
						ReferenceCounted::release
				);
	}

	@Override
	public Mono<U> removeAndGetPrevious(T keySuffix) {
		ByteBuf keySuffixBuf = serializeSuffix(keySuffix);
		ByteBuf keyBuf = toKey(keySuffixBuf.retain());
		return dictionary
				.remove(keyBuf.retain(), LLDictionaryResultType.PREVIOUS_VALUE)
				.map(this::deserialize)
				.doFinally(s -> {
					keyBuf.release();
					keySuffixBuf.release();
				});
	}

	@Override
	public Mono<Boolean> removeAndGetStatus(T keySuffix) {
		ByteBuf keySuffixBuf = serializeSuffix(keySuffix);
		ByteBuf keyBuf = toKey(keySuffixBuf.retain());
		return dictionary
				.remove(keyBuf.retain(), LLDictionaryResultType.PREVIOUS_VALUE_EXISTENCE)
				.map(LLUtils::responseToBoolean)
				.doFinally(s -> {
					keyBuf.release();
					keySuffixBuf.release();
				});
	}

	@Override
	public Flux<Entry<T, U>> getMulti(@Nullable CompositeSnapshot snapshot, Flux<T> keys, boolean existsAlmostCertainly) {
		return dictionary
				.getMulti(resolveSnapshot(snapshot), keys.flatMap(keySuffix -> Mono.fromCallable(() -> {
					ByteBuf keySuffixBuf = serializeSuffix(keySuffix);
					try {
						return toKey(keySuffixBuf.retain());
					} finally {
						keySuffixBuf.release();
					}
				})), existsAlmostCertainly)
				.flatMap(entry -> Mono
						.fromCallable(() -> Map.entry(deserializeSuffix(stripPrefix(entry.getKey(), false)), deserialize(entry.getValue())))
				);
	}

	private Entry<ByteBuf, ByteBuf> serializeEntry(T key, U value) {
		ByteBuf serializedKey = toKey(serializeSuffix(key));
		try {
			ByteBuf serializedValue = serialize(value);
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
							//noinspection unchecked
							var castedEntry = (Entry<ByteBuf, ByteBuf>) uncastedEntry;
							castedEntry.getKey().release();
							castedEntry.getValue().release();
						})
				);
		return dictionary
				.putMulti(serializedEntries, false)
				.then();
	}

	@Override
	public Flux<Entry<T, DatabaseStageEntry<U>>> getAllStages(@Nullable CompositeSnapshot snapshot) {
		return Flux.defer(() -> dictionary.getRangeKeys(resolveSnapshot(snapshot), range.retain()))
				.<Entry<T, DatabaseStageEntry<U>>>map(key -> {
					ByteBuf keySuffixWithExt = stripPrefix(key, false);
					// Don't use "key" under this point ---
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
				})
				.doFirst(() -> range.retain())
				.doFinally(s -> range.release());
	}

	@Override
	public Flux<Entry<T, U>> getAllValues(@Nullable CompositeSnapshot snapshot) {
		return Flux.defer(() -> dictionary.getRange(resolveSnapshot(snapshot), range.retain()))
				.map(serializedEntry -> Map.entry(
						deserializeSuffix(stripPrefix(serializedEntry.getKey(), false)),
						valueSerializer.deserialize(serializedEntry.getValue())
				))
				.doOnDiscard(Entry.class, entry -> {
					//noinspection unchecked
					var castedEntry = (Entry<ByteBuf, ByteBuf>) entry;
					castedEntry.getKey().release();
					castedEntry.getValue().release();
				})
				.doFirst(() -> range.retain())
				.doFinally(s -> range.release());
	}

	@Override
	public Flux<Entry<T, U>> setAllValuesAndGetPrevious(Flux<Entry<T, U>> entries) {
		return Flux
				.usingWhen(
						Mono.just(true),
						b -> getAllValues(null),
						b -> dictionary
								.setRange(range.retain(),
										entries.map(entry ->
												Map.entry(toKey(serializeSuffix(entry.getKey())), serialize(entry.getValue()))
										)
								)
				)
				.doFirst(() -> range.retain())
				.doFinally(s -> range.release());
	}

	@Override
	public Mono<Void> clear() {
		if (range.isAll()) {
			return dictionary
					.clear();
		} else if (range.isSingle()) {
			return Mono
					.defer(() -> dictionary.remove(range.getSingle().retain(), LLDictionaryResultType.VOID))
					.doOnNext(ReferenceCounted::release)
					.then()
					.doFirst(() -> range.getSingle().retain())
					.doFinally(s -> range.getSingle().release());
		} else {
			return Mono
					.defer(() -> dictionary.setRange(range.retain(), Flux.empty()))
					.doFirst(() -> range.retain())
					.doFinally(s -> range.release());
		}
	}

	/**
	 * This method is just a shorter version than valueSerializer::deserialize
	 */
	private U deserialize(ByteBuf bytes) {
		return valueSerializer.deserialize(bytes);
	}

	/**
	 * This method is just a shorter version than valueSerializer::serialize
	 */
	private ByteBuf serialize(U bytes) {
		return valueSerializer.serialize(bytes);
	}
}
