package it.cavallium.dbengine.database.collections;

import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.LLDictionaryResultType;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.serialization.Serializer;
import it.cavallium.dbengine.database.serialization.SerializerFixedBinaryLength;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Function;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Optimized implementation of "DatabaseMapDictionary with SubStageGetterSingle"
 */
public class DatabaseMapDictionary<T, U> extends DatabaseMapDictionaryDeep<T, U, DatabaseStageEntry<U>> {

	private final Serializer<U, byte[]> valueSerializer;

	protected DatabaseMapDictionary(LLDictionary dictionary,
			byte[] prefixKey,
			SerializerFixedBinaryLength<T, byte[]> keySuffixSerializer,
			Serializer<U, byte[]> valueSerializer) {
		super(dictionary, prefixKey, keySuffixSerializer, new SubStageGetterSingle<>(valueSerializer), 0);
		this.valueSerializer = valueSerializer;
	}

	public static <T, U> DatabaseMapDictionary<T, U> simple(LLDictionary dictionary,
			SerializerFixedBinaryLength<T, byte[]> keySerializer,
			Serializer<U, byte[]> valueSerializer) {
		return new DatabaseMapDictionary<>(dictionary, EMPTY_BYTES, keySerializer, valueSerializer);
	}

	public static <T, U> DatabaseMapDictionary<T, U> tail(LLDictionary dictionary,
			byte[] prefixKey,
			SerializerFixedBinaryLength<T, byte[]> keySuffixSerializer,
			Serializer<U, byte[]> valueSerializer) {
		return new DatabaseMapDictionary<>(dictionary, prefixKey, keySuffixSerializer, valueSerializer);
	}

	private byte[] toKey(byte[] suffixKey) {
		assert suffixKeyConsistency(suffixKey.length);
		byte[] key = Arrays.copyOf(keyPrefix, keyPrefix.length + suffixKey.length);
		System.arraycopy(suffixKey, 0, key, keyPrefix.length, suffixKey.length);
		return key;
	}

	@Override
	public Mono<Map<T, U>> get(@Nullable CompositeSnapshot snapshot) {
		return dictionary
				.getRange(resolveSnapshot(snapshot), range)
				.collectMap(
						entry -> deserializeSuffix(stripPrefix(entry.getKey())),
						entry -> deserialize(entry.getValue()),
						HashMap::new);
	}

	@Override
	public Mono<Map<T, U>> setAndGetPrevious(Map<T, U> value) {
		return dictionary
				.setRange(range,
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
				.setRange(range, Flux.empty(), true)
				.collectMap(
						entry -> deserializeSuffix(stripPrefix(entry.getKey())),
						entry -> deserialize(entry.getValue()),
						HashMap::new);
	}

	@Override
	public Mono<Long> leavesCount(@Nullable CompositeSnapshot snapshot, boolean fast) {
		return dictionary.sizeRange(resolveSnapshot(snapshot), range, fast);
	}

	@Override
	public Mono<DatabaseStageEntry<U>> at(@Nullable CompositeSnapshot snapshot, T keySuffix) {
		return Mono
				.just(new DatabaseSingle<>(dictionary, toKey(serializeSuffix(keySuffix)), Serializer.noop()))
				.map(entry -> new DatabaseSingleMapped<>(entry, valueSerializer));
	}

	@Override
	public Mono<U> getValue(@Nullable CompositeSnapshot snapshot, T keySuffix) {
		return dictionary.get(resolveSnapshot(snapshot), toKey(serializeSuffix(keySuffix))).map(this::deserialize);
	}

	@Override
	public Mono<Void> putValue(T keySuffix, U value) {
		return dictionary.put(toKey(serializeSuffix(keySuffix)), serialize(value), LLDictionaryResultType.VOID).then();
	}

	@Override
	public Mono<Boolean> updateValue(T keySuffix, Function<Optional<U>, Optional<U>> updater) {
		return dictionary.update(toKey(serializeSuffix(keySuffix)),
				oldSerialized -> updater.apply(oldSerialized.map(this::deserialize)).map(this::serialize)
		);
	}

	@Override
	public Mono<U> putValueAndGetPrevious(T keySuffix, U value) {
		return dictionary
				.put(toKey(serializeSuffix(keySuffix)), serialize(value), LLDictionaryResultType.PREVIOUS_VALUE)
				.map(this::deserialize);
	}

	@Override
	public Mono<Boolean> putValueAndGetStatus(T keySuffix, U value) {
		return dictionary
				.put(toKey(serializeSuffix(keySuffix)), serialize(value), LLDictionaryResultType.VALUE_CHANGED)
				.map(LLUtils::responseToBoolean);
	}

	@Override
	public Mono<Void> remove(T keySuffix) {
		return dictionary.remove(toKey(serializeSuffix(keySuffix)), LLDictionaryResultType.VOID).then();
	}

	@Override
	public Mono<U> removeAndGetPrevious(T keySuffix) {
		return dictionary
				.remove(toKey(serializeSuffix(keySuffix)), LLDictionaryResultType.PREVIOUS_VALUE)
				.map(this::deserialize);
	}

	@Override
	public Mono<Boolean> removeAndGetStatus(T keySuffix) {
		return dictionary
				.remove(toKey(serializeSuffix(keySuffix)), LLDictionaryResultType.VALUE_CHANGED)
				.map(LLUtils::responseToBoolean);
	}

	@Override
	public Flux<Entry<T, U>> getMulti(@Nullable CompositeSnapshot snapshot, Flux<T> keys) {
		return dictionary
				.getMulti(resolveSnapshot(snapshot), keys.map(keySuffix -> toKey(serializeSuffix(keySuffix))))
				.map(entry -> Map.entry(deserializeSuffix(stripPrefix(entry.getKey())), deserialize(entry.getValue())));
	}

	@Override
	public Mono<Void> putMulti(Flux<Entry<T, U>> entries) {
		return dictionary
				.putMulti(entries
						.map(entry -> Map
								.entry(toKey(serializeSuffix(entry.getKey())), serialize(entry.getValue()))), false)
				.then();
	}

	@Override
	public Flux<Entry<T, DatabaseStageEntry<U>>> getAllStages(@Nullable CompositeSnapshot snapshot) {
		return dictionary
				.getRangeKeys(resolveSnapshot(snapshot), range)
				.map(keySuffix -> Map.entry(deserializeSuffix(stripPrefix(keySuffix)),
						new DatabaseSingleMapped<>(
								new DatabaseSingle<>(dictionary,
										toKey(stripPrefix(keySuffix)),
										Serializer.noop()),
								valueSerializer
						)
				));
	}

	@Override
	public Flux<Entry<T, U>> setAllValuesAndGetPrevious(Flux<Entry<T, U>> entries) {
		return dictionary
				.setRange(range,
						entries.map(entry ->
								Map.entry(toKey(serializeSuffix(entry.getKey())), serialize(entry.getValue()))), true)
				.map(entry -> Map.entry(deserializeSuffix(stripPrefix(entry.getKey())), deserialize(entry.getValue())));
	}

	//todo: temporary wrapper. convert the whole class to buffers
	private U deserialize(byte[] bytes) {
		return valueSerializer.deserialize(bytes);
	}

	//todo: temporary wrapper. convert the whole class to buffers
	private byte[] serialize(U bytes) {
		return valueSerializer.serialize(bytes);
	}
}
