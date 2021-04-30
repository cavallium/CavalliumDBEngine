package it.cavallium.dbengine.database.collections;

import io.netty.buffer.ByteBuf;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.LLDictionaryResultType;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.serialization.Serializer;
import it.cavallium.dbengine.database.serialization.SerializerFixedBinaryLength;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.RocksDBException;
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
		return new DatabaseMapDictionary<>(dictionary, EMPTY_BYTES, keySerializer, valueSerializer);
	}

	public static <T, U> DatabaseMapDictionary<T, U> tail(LLDictionary dictionary,
			ByteBuf prefixKey,
			SerializerFixedBinaryLength<T, ByteBuf> keySuffixSerializer,
			Serializer<U, ByteBuf> valueSerializer) {
		return new DatabaseMapDictionary<>(dictionary, prefixKey, keySuffixSerializer, valueSerializer);
	}

	private ByteBuf toKey(ByteBuf suffixKey) {
		assert suffixKeyConsistency(suffixKey.readableBytes());
		try {
			return LLUtils.directCompositeBuffer(dictionary.getAllocator(), keyPrefix.retain(), suffixKey.retain());
		} finally {
			suffixKey.release();
		}
	}

	@Override
	public Mono<Map<T, U>> get(@Nullable CompositeSnapshot snapshot, boolean existsAlmostCertainly) {
		return dictionary
				.getRange(resolveSnapshot(snapshot), range.retain(), existsAlmostCertainly)
				.collectMap(
						entry -> deserializeSuffix(stripPrefix(entry.getKey())),
						entry -> deserialize(entry.getValue()),
						HashMap::new);
	}

	@Override
	public Mono<Map<T, U>> setAndGetPrevious(Map<T, U> value) {
		return dictionary
				.setRange(range.retain(),
						Flux
								.fromIterable(value.entrySet())
								.map(entry -> Map.entry(serializeSuffix(entry.getKey()), serialize(entry.getValue()))),
						true
				)
				.collectMap(
						entry -> deserializeSuffix(stripPrefix(entry.getKey())),
						entry -> deserialize(entry.getValue()),
						HashMap::new);
	}

	@Override
	public Mono<Map<T, U>> clearAndGetPrevious() {
		return dictionary
				.setRange(range.retain(), Flux.empty(), true)
				.collectMap(
						entry -> deserializeSuffix(stripPrefix(entry.getKey())),
						entry -> deserialize(entry.getValue()),
						HashMap::new);
	}

	@Override
	public Mono<Long> leavesCount(@Nullable CompositeSnapshot snapshot, boolean fast) {
		return dictionary.sizeRange(resolveSnapshot(snapshot), range.retain(), fast);
	}

	@Override
	public Mono<Boolean> isEmpty(@Nullable CompositeSnapshot snapshot) {
		return dictionary.isRangeEmpty(resolveSnapshot(snapshot), range.retain());
	}

	@Override
	public Mono<DatabaseStageEntry<U>> at(@Nullable CompositeSnapshot snapshot, T keySuffix) {
		ByteBuf keySuffixBuf = serializeSuffix(keySuffix);
		ByteBuf keyBuf = toKey(keySuffixBuf.retain());
		return Mono
				.fromSupplier(() -> new DatabaseSingle<>(dictionary, keyBuf.retain(), Serializer.noop()))
				.<DatabaseStageEntry<U>>map(entry -> new DatabaseSingleMapped<>(entry, valueSerializer))
				.doFinally(s -> {
					keyBuf.release();
					keySuffixBuf.release();
				});
	}

	@Override
	public Mono<U> getValue(@Nullable CompositeSnapshot snapshot, T keySuffix, boolean existsAlmostCertainly) {
		ByteBuf keySuffixBuf = serializeSuffix(keySuffix);
		ByteBuf keyBuf = toKey(keySuffixBuf.retain());
		return dictionary
				.get(resolveSnapshot(snapshot), keyBuf.retain(), existsAlmostCertainly)
				.map(this::deserialize)
				.doFinally(s -> {
					keyBuf.release();
					keySuffixBuf.release();
				});
	}

	@Override
	public Mono<Void> putValue(T keySuffix, U value) {
		ByteBuf keySuffixBuf = serializeSuffix(keySuffix);
		ByteBuf keyBuf = toKey(keySuffixBuf.retain());
		ByteBuf valueBuf = serialize(value);
		return dictionary.put(keyBuf.retain(), valueBuf.retain(), LLDictionaryResultType.VOID).doFinally(s -> {
			keyBuf.release();
			keySuffixBuf.release();
			valueBuf.release();
		}).then();
	}

	@Override
	public Mono<Boolean> updateValue(T keySuffix,
			boolean existsAlmostCertainly,
			Function<@Nullable U, @Nullable U> updater) {
		ByteBuf keySuffixBuf = serializeSuffix(keySuffix);
		ByteBuf keyBuf = toKey(keySuffixBuf.retain());
		return dictionary.update(keyBuf.retain(), oldSerialized -> {
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
		}, existsAlmostCertainly).doFinally(s -> {
			keyBuf.release();
			keySuffixBuf.release();
		});
	}

	@Override
	public Mono<U> putValueAndGetPrevious(T keySuffix, U value) {
		ByteBuf keySuffixBuf = serializeSuffix(keySuffix);
		ByteBuf keyBuf = toKey(keySuffixBuf.retain());
		ByteBuf valueBuf = serialize(value);
		return dictionary
				.put(keyBuf.retain(), valueBuf.retain(), LLDictionaryResultType.PREVIOUS_VALUE)
				.map(this::deserialize)
				.doFinally(s -> {
					keyBuf.release();
					keySuffixBuf.release();
					valueBuf.release();
				});
	}

	@Override
	public Mono<Boolean> putValueAndGetStatus(T keySuffix, U value) {
		ByteBuf keySuffixBuf = serializeSuffix(keySuffix);
		ByteBuf keyBuf = toKey(keySuffixBuf.retain());
		ByteBuf valueBuf = serialize(value);
		return dictionary
				.put(keyBuf.retain(), valueBuf.retain(), LLDictionaryResultType.PREVIOUS_VALUE_EXISTENCE)
				.map(LLUtils::responseToBoolean)
				.doFinally(s -> {
					keyBuf.release();
					keySuffixBuf.release();
					valueBuf.release();
				});
	}

	@Override
	public Mono<Void> remove(T keySuffix) {
		ByteBuf keySuffixBuf = serializeSuffix(keySuffix);
		ByteBuf keyBuf = toKey(keySuffixBuf.retain());
		return dictionary.remove(keyBuf.retain(), LLDictionaryResultType.VOID).doFinally(s -> {
			keyBuf.release();
			keySuffixBuf.release();
		}).then();
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
				.flatMap(entry -> Mono.fromCallable(() -> Map.entry(deserializeSuffix(stripPrefix(entry.getKey())), deserialize(entry.getValue()))));
	}

	@Override
	public Mono<Void> putMulti(Flux<Entry<T, U>> entries) {
		return dictionary
				.putMulti(entries.flatMap(entry -> Mono.fromCallable(() -> Map.entry(toKey(serializeSuffix(entry.getKey())),
						serialize(entry.getValue())
				))), false)
				.then();
	}

	@Override
	public Flux<Entry<T, DatabaseStageEntry<U>>> getAllStages(@Nullable CompositeSnapshot snapshot) {
		return dictionary
				.getRangeKeys(resolveSnapshot(snapshot), range.retain())
				.map(key -> Map.entry(deserializeSuffix(stripPrefix(key)),
						new DatabaseSingleMapped<>(
								new DatabaseSingle<>(dictionary,
										toKey(stripPrefix(key)),
										Serializer.noop()),
								valueSerializer
						)
				));
	}

	@Override
	public Flux<Entry<T, U>> getAllValues(@Nullable CompositeSnapshot snapshot) {
		return dictionary
				.getRange(resolveSnapshot(snapshot), range.retain())
				.map(serializedEntry -> Map.entry(
						deserializeSuffix(stripPrefix(serializedEntry.getKey())),
						valueSerializer.deserialize(serializedEntry.getValue())
				));
	}

	@Override
	public Flux<Entry<T, U>> setAllValuesAndGetPrevious(Flux<Entry<T, U>> entries) {
		return dictionary
				.setRange(range.retain(),
						entries.map(entry ->
								Map.entry(toKey(serializeSuffix(entry.getKey())), serialize(entry.getValue()))), true)
				.map(entry -> Map.entry(deserializeSuffix(stripPrefix(entry.getKey())), deserialize(entry.getValue())));
	}

	@Override
	public Mono<Void> clear() {
		if (range.isAll()) {
			return dictionary
					.clear();
		} else if (range.isSingle()) {
			return dictionary
					.remove(range.getSingle().retain(), LLDictionaryResultType.VOID)
					.then();
		} else {
			return dictionary
					.setRange(range.retain(), Flux.empty(), false)
					.then();
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

	@Override
	public void release() {
		super.release();
	}
}
