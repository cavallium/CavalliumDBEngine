package it.cavallium.dbengine.database.collections;

import static it.cavallium.dbengine.database.collections.DatabaseMapDictionaryDeep.EMPTY_BYTES;

import com.google.common.primitives.Ints;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.collections.Joiner.ValueGetter;
import it.cavallium.dbengine.database.collections.JoinerBlocking.ValueGetterBlocking;
import it.cavallium.dbengine.database.serialization.Serializer;
import it.cavallium.dbengine.database.serialization.SerializerFixedBinaryLength;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Function;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@SuppressWarnings("unused")
public class DatabaseMapDictionaryHashed<T, U, TH> implements DatabaseStageMap<T, U, DatabaseStageEntry<U>> {

	private final DatabaseMapDictionary<TH, Entry<T, U>> subDictionary;
	private final Function<T, TH> keySuffixHashFunction;
	private final Function<T, ValueMapper<T, U>> valueMapper;

	protected DatabaseMapDictionaryHashed(LLDictionary dictionary,
			byte[] prefixKey,
			Serializer<T, byte[]> keySuffixSerializer,
			Serializer<U, byte[]> valueSerializer,
			Function<T, TH> keySuffixHashFunction,
			SerializerFixedBinaryLength<TH, byte[]> keySuffixHashSerializer) {
		ValueWithHashSerializer<T, U> valueWithHashSerializer = new ValueWithHashSerializer<>(keySuffixSerializer,
				valueSerializer
		);
		this.valueMapper = ValueMapper::new;
		this.subDictionary = DatabaseMapDictionary.tail(dictionary,
				prefixKey,
				keySuffixHashSerializer, valueWithHashSerializer
		);
		this.keySuffixHashFunction = keySuffixHashFunction;
	}

	private static class ValueWithHashSerializer<T, U> implements Serializer<Entry<T, U>, byte[]> {

		private final Serializer<T, byte[]> keySuffixSerializer;
		private final Serializer<U, byte[]> valueSerializer;

		private ValueWithHashSerializer(Serializer<T, byte[]> keySuffixSerializer, Serializer<U, byte[]> valueSerializer) {
			this.keySuffixSerializer = keySuffixSerializer;
			this.valueSerializer = valueSerializer;
		}

		@Override
		public @NotNull Entry<T, U> deserialize(byte @NotNull [] serialized) {
			int keySuffixLength = Ints.fromBytes(serialized[0], serialized[1], serialized[2], serialized[3]);
			T keySuffix = keySuffixSerializer.deserialize(Arrays.copyOfRange(serialized, 4, keySuffixLength));
			U value = valueSerializer.deserialize(Arrays.copyOfRange(serialized,
					4 + keySuffixLength,
					serialized.length - (4 + keySuffixLength)
			));
			return Map.entry(keySuffix, value);
		}

		@Override
		public byte @NotNull [] serialize(@NotNull Entry<T, U> deserialized) {
			byte[] keySuffix = keySuffixSerializer.serialize(deserialized.getKey());
			byte[] value = valueSerializer.serialize(deserialized.getValue());
			byte[] result = new byte[4 + keySuffix.length + value.length];
			byte[] keySuffixLen = Ints.toByteArray(keySuffix.length);
			System.arraycopy(keySuffixLen, 0, result, 0, 4);
			System.arraycopy(keySuffix, 0, result, 4, keySuffix.length);
			System.arraycopy(value, 0, result, 4 + keySuffix.length, value.length);
			return result;
		}
	}

	private static class ValueMapper<T, U> implements Serializer<U, Entry<T, U>> {

		private final T key;

		public ValueMapper(T key) {
			this.key = key;
		}

		@Override
		public @NotNull U deserialize(@NotNull Entry<T, U> serialized) {
			return serialized.getValue();
		}

		@Override
		public @NotNull Entry<T, U> serialize(@NotNull U deserialized) {
			return Map.entry(key, deserialized);
		}
	}

	public static <T, U, UH> DatabaseMapDictionaryHashed<T, U, UH> simple(LLDictionary dictionary,
			Serializer<T, byte[]> keySerializer,
			Serializer<U, byte[]> valueSerializer,
			Function<T, UH> keyHashFunction,
			SerializerFixedBinaryLength<UH, byte[]> keyHashSerializer) {
		return new DatabaseMapDictionaryHashed<>(dictionary,
				EMPTY_BYTES,
				keySerializer,
				valueSerializer,
				keyHashFunction,
				keyHashSerializer
		);
	}

	public static <T, U, UH> DatabaseMapDictionaryHashed<T, U, UH> tail(LLDictionary dictionary,
			byte[] prefixKey,
			Serializer<T, byte[]> keySuffixSerializer,
			Serializer<U, byte[]> valueSerializer,
			Function<T, UH> keySuffixHashFunction,
			SerializerFixedBinaryLength<UH, byte[]> keySuffixHashSerializer) {
		return new DatabaseMapDictionaryHashed<>(dictionary,
				prefixKey,
				keySuffixSerializer,
				valueSerializer,
				keySuffixHashFunction,
				keySuffixHashSerializer
		);
	}

	private Map<TH, Entry<T, U>> serializeMap(Map<T, U> map) {
		var newMap = new HashMap<TH, Entry<T, U>>(map.size());
		map.forEach((key, value) -> newMap.put(keySuffixHashFunction.apply(key), Map.entry(key, value)));
		return newMap;
	}

	private Map<T, U> deserializeMap(Map<TH, Entry<T, U>> map) {
		var newMap = new HashMap<T, U>(map.size());
		map.forEach((key, value) -> newMap.put(value.getKey(), value.getValue()));
		return newMap;
	}

	@Override
	public Mono<Map<T, U>> get(@Nullable CompositeSnapshot snapshot) {
		return subDictionary.get(snapshot).map(this::deserializeMap);
	}

	@Override
	public Mono<Map<T, U>> getOrDefault(@Nullable CompositeSnapshot snapshot, Mono<Map<T, U>> defaultValue) {
		return this.get(snapshot).switchIfEmpty(defaultValue);
	}

	@Override
	public Mono<Void> set(Map<T, U> map) {
		return Mono.fromSupplier(() -> this.serializeMap(map)).flatMap(subDictionary::set);
	}

	@Override
	public Mono<Boolean> setAndGetStatus(Map<T, U> map) {
		return Mono.fromSupplier(() -> this.serializeMap(map)).flatMap(subDictionary::setAndGetStatus);
	}

	@Override
	public Mono<Boolean> update(Function<Optional<Map<T, U>>, Optional<Map<T, U>>> updater) {
		return subDictionary.update(old -> updater.apply(old.map(this::deserializeMap)).map(this::serializeMap));
	}

	@Override
	public Mono<Boolean> clearAndGetStatus() {
		return subDictionary.clearAndGetStatus();
	}

	@Override
	public Mono<Void> close() {
		return subDictionary.close();
	}

	@Override
	public Mono<Boolean> isEmpty(@Nullable CompositeSnapshot snapshot) {
		return subDictionary.isEmpty(snapshot);
	}

	@Override
	public DatabaseStageEntry<Map<T, U>> entry() {
		return this;
	}

	@Override
	public Mono<DatabaseStageEntry<U>> at(@Nullable CompositeSnapshot snapshot, T key) {
		return subDictionary
				.at(snapshot, keySuffixHashFunction.apply(key))
				.map(entry -> new DatabaseSingleMapped<>(entry, valueMapper.apply(key)));
	}

	@Override
	public Mono<U> getValue(@Nullable CompositeSnapshot snapshot, T key, boolean existsAlmostCertainly) {
		return subDictionary
				.getValue(snapshot, keySuffixHashFunction.apply(key), existsAlmostCertainly)
				.map(Entry::getValue);
	}

	@Override
	public Mono<U> getValue(@Nullable CompositeSnapshot snapshot, T key) {
		return subDictionary.getValue(snapshot, keySuffixHashFunction.apply(key)).map(Entry::getValue);
	}

	@Override
	public Mono<U> getValueOrDefault(@Nullable CompositeSnapshot snapshot, T key, Mono<U> defaultValue) {
		return subDictionary
				.getValueOrDefault(snapshot, keySuffixHashFunction.apply(key), defaultValue.map(v -> Map.entry(key, v)))
				.map(Entry::getValue);
	}

	@Override
	public Mono<Void> putValue(T key, U value) {
		return subDictionary.putValue(keySuffixHashFunction.apply(key), Map.entry(key, value));
	}

	@Override
	public Mono<Boolean> updateValue(T key, boolean existsAlmostCertainly, Function<Optional<U>, Optional<U>> updater) {
		return subDictionary.updateValue(keySuffixHashFunction.apply(key),
				existsAlmostCertainly,
				old -> updater.apply(old.map(Entry::getValue)).map(newV -> Map.entry(key, newV))
		);
	}

	@Override
	public Mono<Boolean> updateValue(T key, Function<Optional<U>, Optional<U>> updater) {
		return subDictionary.updateValue(keySuffixHashFunction.apply(key),
				old -> updater.apply(old.map(Entry::getValue)).map(newV -> Map.entry(key, newV))
		);
	}

	@Override
	public Mono<U> putValueAndGetPrevious(T key, U value) {
		return subDictionary
				.putValueAndGetPrevious(keySuffixHashFunction.apply(key), Map.entry(key, value))
				.map(Entry::getValue);
	}

	@Override
	public Mono<Boolean> putValueAndGetStatus(T key, U value) {
		return subDictionary
				.putValueAndGetStatus(keySuffixHashFunction.apply(key), Map.entry(key, value));
	}

	@Override
	public Mono<Void> remove(T key) {
		return subDictionary.remove(keySuffixHashFunction.apply(key));
	}

	@Override
	public Mono<U> removeAndGetPrevious(T key) {
		return subDictionary.removeAndGetPrevious(keySuffixHashFunction.apply(key)).map(Entry::getValue);
	}

	@Override
	public Mono<Boolean> removeAndGetStatus(T key) {
		return subDictionary.removeAndGetStatus(keySuffixHashFunction.apply(key));
	}

	@Override
	public Flux<Entry<T, U>> getMulti(@Nullable CompositeSnapshot snapshot, Flux<T> keys, boolean existsAlmostCertainly) {
		return subDictionary
				.getMulti(snapshot, keys.map(keySuffixHashFunction), existsAlmostCertainly)
				.map(Entry::getValue);
	}

	@Override
	public Flux<Entry<T, U>> getMulti(@Nullable CompositeSnapshot snapshot, Flux<T> keys) {
		return subDictionary
				.getMulti(snapshot, keys.map(keySuffixHashFunction))
				.map(Entry::getValue);
	}

	@Override
	public Mono<Void> putMulti(Flux<Entry<T, U>> entries) {
		return subDictionary
				.putMulti(entries.map(entry -> Map.entry(keySuffixHashFunction.apply(entry.getKey()),
						Map.entry(entry.getKey(), entry.getValue())
				)));
	}

	@Override
	public Flux<Entry<T, DatabaseStageEntry<U>>> getAllStages(@Nullable CompositeSnapshot snapshot) {
		return subDictionary
				.getAllStages(snapshot)
				.flatMap(hashEntry -> hashEntry
						.getValue()
						.get(snapshot)
						.map(entry -> Map.entry(entry.getKey(),
								new DatabaseSingleMapped<>(hashEntry.getValue(), valueMapper.apply(entry.getKey()))
						))
				);
	}

	@Override
	public Flux<Entry<T, U>> getAllValues(@Nullable CompositeSnapshot snapshot) {
		return subDictionary.getAllValues(snapshot).map(Entry::getValue);
	}

	@Override
	public Mono<Void> setAllValues(Flux<Entry<T, U>> entries) {
		return subDictionary
				.setAllValues(entries.map(entry -> Map.entry(keySuffixHashFunction.apply(entry.getKey()), entry)));
	}

	@Override
	public Flux<Entry<T, U>> setAllValuesAndGetPrevious(Flux<Entry<T, U>> entries) {
		return subDictionary
				.setAllValuesAndGetPrevious(entries.map(entry -> Map.entry(keySuffixHashFunction.apply(entry.getKey()), entry)))
				.map(Entry::getValue);
	}

	@Override
	public Mono<Void> clear() {
		return subDictionary.clear();
	}

	@Override
	public Mono<Void> replaceAllValues(boolean canKeysChange, Function<Entry<T, U>, Mono<Entry<T, U>>> entriesReplacer) {
		return subDictionary.replaceAllValues(canKeysChange,
				entry -> entriesReplacer
						.apply(entry.getValue())
						.map(result -> Map.entry(keySuffixHashFunction.apply(result.getKey()), result))
		);
	}

	@Override
	public Mono<Void> replaceAll(Function<Entry<T, DatabaseStageEntry<U>>, Mono<Void>> entriesReplacer) {
		return subDictionary.replaceAll(hashEntry -> hashEntry
				.getValue()
				.get(null)
				.flatMap(entry -> entriesReplacer.apply(Map.entry(entry.getKey(),
						new DatabaseSingleMapped<>(hashEntry.getValue(), valueMapper.apply(entry.getKey()))
				))));
	}

	@Override
	public Mono<Map<T, U>> setAndGetPrevious(Map<T, U> value) {
		return Mono
				.fromSupplier(() -> this.serializeMap(value))
				.flatMap(subDictionary::setAndGetPrevious)
				.map(this::deserializeMap);
	}

	@Override
	public Mono<Boolean> update(Function<Optional<Map<T, U>>, Optional<Map<T, U>>> updater,
			boolean existsAlmostCertainly) {
		return subDictionary
				.update(item -> updater.apply(item.map(this::deserializeMap)).map(this::serializeMap), existsAlmostCertainly);
	}

	@Override
	public Mono<Map<T, U>> clearAndGetPrevious() {
		return subDictionary
				.clearAndGetPrevious()
				.map(this::deserializeMap);
	}

	@Override
	public Mono<Map<T, U>> get(@Nullable CompositeSnapshot snapshot, boolean existsAlmostCertainly) {
		return subDictionary
				.get(snapshot, existsAlmostCertainly)
				.map(this::deserializeMap);
	}

	@Override
	public Mono<Long> leavesCount(@Nullable CompositeSnapshot snapshot, boolean fast) {
		return subDictionary.leavesCount(snapshot, fast);
	}

	@Override
	public ValueGetterBlocking<T, U> getDbValueGetter(@Nullable CompositeSnapshot snapshot) {
		ValueGetterBlocking<TH, Entry<T, U>> getter = subDictionary.getDbValueGetter(snapshot);
		return key -> getter.get(keySuffixHashFunction.apply(key)).getValue();
	}

	@Override
	public ValueGetter<T, U> getAsyncDbValueGetter(@Nullable CompositeSnapshot snapshot) {
		ValueGetter<TH, Entry<T, U>> getter = subDictionary.getAsyncDbValueGetter(snapshot);
		return key -> getter.get(keySuffixHashFunction.apply(key)).map(Entry::getValue);
	}
}
