package it.cavallium.dbengine.database.collections;

import io.netty.buffer.api.Buffer;
import io.netty.buffer.api.BufferAllocator;
import io.netty.buffer.api.Send;
import it.cavallium.dbengine.client.BadBlock;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.Delta;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.UpdateMode;
import it.cavallium.dbengine.database.collections.ValueGetter;
import it.cavallium.dbengine.database.serialization.Serializer;
import it.cavallium.dbengine.database.serialization.SerializerFixedBinaryLength;
import it.unimi.dsi.fastutil.objects.ObjectArraySet;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectSets;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuple3;
import reactor.util.function.Tuples;

@SuppressWarnings("unused")
public class DatabaseMapDictionaryHashed<T, U, TH> implements DatabaseStageMap<T, U, DatabaseStageEntry<U>> {

	private final BufferAllocator alloc;
	private final DatabaseMapDictionary<TH, ObjectArraySet<Entry<T, U>>> subDictionary;
	private final Function<T, TH> keySuffixHashFunction;

	protected DatabaseMapDictionaryHashed(LLDictionary dictionary,
			Send<Buffer> prefixKey,
			Serializer<T, Send<Buffer>> keySuffixSerializer,
			Serializer<U, Send<Buffer>> valueSerializer,
			Function<T, TH> keySuffixHashFunction,
			SerializerFixedBinaryLength<TH, Buffer> keySuffixHashSerializer) {
		if (dictionary.getUpdateMode().block() != UpdateMode.ALLOW) {
			throw new IllegalArgumentException("Hashed maps only works when UpdateMode is ALLOW");
		}
		this.alloc = dictionary.getAllocator();
		ValueWithHashSerializer<T, U> valueWithHashSerializer
				= new ValueWithHashSerializer<>(alloc, keySuffixSerializer, valueSerializer);
		ValuesSetSerializer<Entry<T, U>> valuesSetSerializer
				= new ValuesSetSerializer<>(alloc, valueWithHashSerializer);
		this.subDictionary = DatabaseMapDictionary.tail(dictionary,
				prefixKey,
				keySuffixHashSerializer,
				valuesSetSerializer
		);
		this.keySuffixHashFunction = keySuffixHashFunction;
	}

	public static <T, U, UH> DatabaseMapDictionaryHashed<T, U, UH> simple(LLDictionary dictionary,
			Serializer<T, Buffer> keySerializer,
			Serializer<U, Buffer> valueSerializer,
			Function<T, UH> keyHashFunction,
			SerializerFixedBinaryLength<UH, Buffer> keyHashSerializer) {
		return new DatabaseMapDictionaryHashed<>(
				dictionary,
				dictionary.getAllocator().buffer(0),
				keySerializer,
				valueSerializer,
				keyHashFunction,
				keyHashSerializer
		);
	}

	public static <T, U, UH> DatabaseMapDictionaryHashed<T, U, UH> tail(LLDictionary dictionary,
			Buffer prefixKey,
			Serializer<T, Buffer> keySuffixSerializer,
			Serializer<U, Buffer> valueSerializer,
			Function<T, UH> keySuffixHashFunction,
			SerializerFixedBinaryLength<UH, Buffer> keySuffixHashSerializer) {
		return new DatabaseMapDictionaryHashed<>(dictionary,
				prefixKey,
				keySuffixSerializer,
				valueSerializer,
				keySuffixHashFunction,
				keySuffixHashSerializer
		);
	}

	private Map<TH, ObjectArraySet<Entry<T, U>>> serializeMap(Map<T, U> map) {
		var newMap = new HashMap<TH, ObjectArraySet<Entry<T, U>>>(map.size());
		map.forEach((key, value) -> newMap.compute(keySuffixHashFunction.apply(key), (hash, prev) -> {
			if (prev == null) {
				prev = new ObjectArraySet<>();
			}
			prev.add(Map.entry(key, value));
			return prev;
		}));
		return newMap;
	}

	private Map<T, U> deserializeMap(Map<TH, ObjectArraySet<Entry<T, U>>> map) {
		var newMap = new HashMap<T, U>(map.size());
		map.forEach((hash, set) -> set.forEach(entry -> newMap.put(entry.getKey(), entry.getValue())));
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
	public Mono<Boolean> setAndGetChanged(Map<T, U> map) {
		return Mono.fromSupplier(() -> this.serializeMap(map)).flatMap(subDictionary::setAndGetChanged).single();
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
	public Flux<BadBlock> badBlocks() {
		return this.subDictionary.badBlocks();
	}

	@Override
	public void release() {
		this.subDictionary.release();
	}

	@Override
	public Mono<DatabaseStageEntry<U>> at(@Nullable CompositeSnapshot snapshot, T key) {
		return this
				.atPrivate(snapshot, key, keySuffixHashFunction.apply(key))
				.map(cast -> cast);
	}

	private Mono<DatabaseSingleBucket<T, U, TH>> atPrivate(@Nullable CompositeSnapshot snapshot, T key, TH hash) {
		return subDictionary
				.at(snapshot, hash)
				.map(entry -> new DatabaseSingleBucket<>(entry, key));
	}

	@Override
	public Mono<UpdateMode> getUpdateMode() {
		return subDictionary.getUpdateMode();
	}

	@Override
	public Flux<Entry<T, DatabaseStageEntry<U>>> getAllStages(@Nullable CompositeSnapshot snapshot) {
		return subDictionary
				.getAllValues(snapshot)
				.map(Entry::getValue)
				.map(Collections::unmodifiableSet)
				.flatMap(bucket -> Flux
						.fromIterable(bucket)
						.map(Entry::getKey)
						.flatMap(key -> this.at(snapshot, key).map(stage -> Map.entry(key, stage)))
				);
	}

	@Override
	public Flux<Entry<T, U>> getAllValues(@Nullable CompositeSnapshot snapshot) {
		return subDictionary
				.getAllValues(snapshot)
				.map(Entry::getValue)
				.map(Collections::unmodifiableSet)
				.concatMapIterable(list -> list);
	}

	@Override
	public Flux<Entry<T, U>> setAllValuesAndGetPrevious(Flux<Entry<T, U>> entries) {
		return entries
				.flatMap(entry -> Flux.usingWhen(
						this.at(null, entry.getKey()),
						stage -> stage
								.setAndGetPrevious(entry.getValue())
								.map(prev -> Map.entry(entry.getKey(), prev)),
						stage -> Mono.fromRunnable(stage::release)
				));
	}

	@Override
	public Mono<Void> clear() {
		return subDictionary.clear();
	}

	@Override
	public Mono<Map<T, U>> setAndGetPrevious(Map<T, U> value) {
		return Mono
				.fromSupplier(() -> this.serializeMap(value))
				.flatMap(subDictionary::setAndGetPrevious)
				.map(this::deserializeMap);
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
		ValueGetterBlocking<TH, ObjectArraySet<Entry<T, U>>> getter = subDictionary.getDbValueGetter(snapshot);
		return key -> extractValue(getter.get(keySuffixHashFunction.apply(key)), key);
	}

	@Override
	public ValueGetter<T, U> getAsyncDbValueGetter(@Nullable CompositeSnapshot snapshot) {
		ValueGetter<TH, ObjectArraySet<Entry<T, U>>> getter = subDictionary.getAsyncDbValueGetter(snapshot);
		return key -> getter
				.get(keySuffixHashFunction.apply(key))
				.flatMap(set -> this.extractValueTransformation(set, key));
	}

	private Mono<U> extractValueTransformation(ObjectArraySet<Entry<T, U>> entries, T key) {
		return Mono.fromCallable(() -> extractValue(entries, key));
	}

	@Nullable
	private U extractValue(ObjectArraySet<Entry<T, U>> entries, T key) {
		if (entries == null) return null;
		for (Entry<T, U> entry : entries) {
			if (Objects.equals(entry.getKey(), key)) {
				return entry.getValue();
			}
		}
		return null;
	}

	@NotNull
	private ObjectArraySet<Entry<T, U>> insertValueOrCreate(@Nullable ObjectArraySet<Entry<T, U>> entries, T key, U value) {
		if (entries != null) {
			var clonedEntries = entries.clone();
			clonedEntries.add(Map.entry(key, value));
			return clonedEntries;
		} else {
			var oas = new ObjectArraySet<Entry<T, U>>(1);
			oas.add(Map.entry(key, value));
			return oas;
		}
	}

	@Nullable
	private Set<Entry<T, U>> removeValueOrDelete(@Nullable ObjectArraySet<Entry<T, U>> entries, T key) {
		if (entries != null) {
			var clonedEntries = entries.clone();
			var it = clonedEntries.iterator();
			while (it.hasNext()) {
				var entry = it.next();
				if (Objects.equals(entry.getKey(), key)) {
					it.remove();
					break;
				}
			}
			if (clonedEntries.size() == 0) {
				return null;
			} else {
				return clonedEntries;
			}
		} else {
			return null;
		}
	}
}
