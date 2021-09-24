package it.cavallium.dbengine.database.collections;

import io.net5.buffer.api.Buffer;
import io.net5.buffer.api.BufferAllocator;
import io.net5.buffer.api.Drop;
import io.net5.buffer.api.Owned;
import io.net5.buffer.api.Send;
import it.cavallium.dbengine.client.BadBlock;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.LLEntry;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.LiveResourceSupport;
import it.cavallium.dbengine.database.UpdateMode;
import it.cavallium.dbengine.database.serialization.Serializer;
import it.cavallium.dbengine.database.serialization.SerializerFixedBinaryLength;
import it.unimi.dsi.fastutil.objects.ObjectArraySet;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@SuppressWarnings("unused")
public class DatabaseMapDictionaryHashed<T, U, TH> extends
		LiveResourceSupport<DatabaseStage<Map<T, U>>, DatabaseMapDictionaryHashed<T, U, TH>>
		implements DatabaseStageMap<T, U, DatabaseStageEntry<U>> {

	private final BufferAllocator alloc;
	private final Function<T, TH> keySuffixHashFunction;

	private DatabaseMapDictionary<TH, ObjectArraySet<Entry<T, U>>> subDictionary;

	protected DatabaseMapDictionaryHashed(LLDictionary dictionary,
			@NotNull Send<Buffer> prefixKey,
			Serializer<T> keySuffixSerializer,
			Serializer<U> valueSerializer,
			Function<T, TH> keySuffixHashFunction,
			SerializerFixedBinaryLength<TH> keySuffixHashSerializer,
			Drop<DatabaseMapDictionaryHashed<T, U, TH>> drop) {
		super(new DatabaseMapDictionaryHashed.CloseOnDrop<>(drop));
		if (dictionary.getUpdateMode().block() != UpdateMode.ALLOW) {
			throw new IllegalArgumentException("Hashed maps only works when UpdateMode is ALLOW");
		}
		this.alloc = dictionary.getAllocator();
		ValueWithHashSerializer<T, U> valueWithHashSerializer
				= new ValueWithHashSerializer<>(alloc, keySuffixSerializer, valueSerializer);
		ValuesSetSerializer<Entry<T, U>> valuesSetSerializer
				= new ValuesSetSerializer<>(alloc, valueWithHashSerializer);
		this.subDictionary = DatabaseMapDictionary.tail(dictionary, prefixKey, keySuffixHashSerializer,
				valuesSetSerializer, d -> {});
		this.keySuffixHashFunction = keySuffixHashFunction;
	}

	private DatabaseMapDictionaryHashed(BufferAllocator alloc,
			Function<T, TH> keySuffixHashFunction,
			Send<DatabaseStage<Map<TH, ObjectArraySet<Entry<T, U>>>>> subDictionary,
			Drop<DatabaseMapDictionaryHashed<T, U, TH>> drop) {
		super(new CloseOnDrop<>(drop));
		this.alloc = alloc;
		this.keySuffixHashFunction = keySuffixHashFunction;

		this.subDictionary = (DatabaseMapDictionary<TH, ObjectArraySet<Entry<T, U>>>) subDictionary.receive();
	}

	public static <T, U, UH> DatabaseMapDictionaryHashed<T, U, UH> simple(LLDictionary dictionary,
			Serializer<T> keySerializer,
			Serializer<U> valueSerializer,
			Function<T, UH> keyHashFunction,
			SerializerFixedBinaryLength<UH> keyHashSerializer,
			Drop<DatabaseMapDictionaryHashed<T, U, UH>> drop) {
		return new DatabaseMapDictionaryHashed<>(
				dictionary,
				LLUtils.empty(dictionary.getAllocator()),
				keySerializer,
				valueSerializer,
				keyHashFunction,
				keyHashSerializer,
				drop
		);
	}

	public static <T, U, UH> DatabaseMapDictionaryHashed<T, U, UH> tail(LLDictionary dictionary,
			Send<Buffer> prefixKey,
			Serializer<T> keySuffixSerializer,
			Serializer<U> valueSerializer,
			Function<T, UH> keySuffixHashFunction,
			SerializerFixedBinaryLength<UH> keySuffixHashSerializer,
			Drop<DatabaseMapDictionaryHashed<T, U, UH>> drop) {
		return new DatabaseMapDictionaryHashed<>(dictionary,
				prefixKey,
				keySuffixSerializer,
				valueSerializer,
				keySuffixHashFunction,
				keySuffixHashSerializer,
				drop
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
	public Mono<DatabaseStageEntry<U>> at(@Nullable CompositeSnapshot snapshot, T key) {
		return this
				.atPrivate(snapshot, key, keySuffixHashFunction.apply(key))
				.map(cast -> cast);
	}

	private Mono<DatabaseSingleBucket<T, U, TH>> atPrivate(@Nullable CompositeSnapshot snapshot, T key, TH hash) {
		return subDictionary
				.at(snapshot, hash)
				.map(entry -> new DatabaseSingleBucket<>(entry, key, d -> {}));
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
				.flatMap(entry -> LLUtils.usingResource(this.at(null, entry.getKey()),
						stage -> stage
								.setAndGetPrevious(entry.getValue())
								.map(prev -> Map.entry(entry.getKey(), prev)), true)
				);
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

	@Override
	protected RuntimeException createResourceClosedException() {
		throw new IllegalStateException("Closed");
	}

	@Override
	protected Owned<DatabaseMapDictionaryHashed<T, U, TH>> prepareSend() {
		var subDictionary = this.subDictionary.send();
		return drop -> new DatabaseMapDictionaryHashed<>(alloc, keySuffixHashFunction, subDictionary, drop);
	}

	@Override
	protected void makeInaccessible() {
		this.subDictionary = null;
	}

	private static class CloseOnDrop<T, U, TH> implements Drop<DatabaseMapDictionaryHashed<T,U,TH>> {

		private final Drop<DatabaseMapDictionaryHashed<T,U,TH>> delegate;

		public CloseOnDrop(Drop<DatabaseMapDictionaryHashed<T,U,TH>> drop) {
			if (drop instanceof CloseOnDrop<T, U, TH> closeOnDrop) {
				this.delegate = closeOnDrop.delegate;
			} else {
				this.delegate = drop;
			}
		}

		@Override
		public void drop(DatabaseMapDictionaryHashed<T, U, TH> obj) {
			obj.subDictionary.close();
			delegate.drop(obj);
		}
	}
}
