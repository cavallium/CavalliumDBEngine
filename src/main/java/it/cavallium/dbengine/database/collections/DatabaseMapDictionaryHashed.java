package it.cavallium.dbengine.database.collections;

import io.netty5.buffer.api.BufferAllocator;
import io.netty5.buffer.api.Drop;
import io.netty5.buffer.api.Owned;
import io.netty5.util.Send;
import it.cavallium.dbengine.client.BadBlock;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.BufSupplier;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.LLUtils;
import io.netty5.buffer.api.internal.ResourceSupport;
import it.cavallium.dbengine.database.SubStageEntry;
import it.cavallium.dbengine.database.UpdateMode;
import it.cavallium.dbengine.database.serialization.Serializer;
import it.cavallium.dbengine.database.serialization.SerializerFixedBinaryLength;
import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectSortedMap;
import it.unimi.dsi.fastutil.objects.ObjectArraySet;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@SuppressWarnings("unused")
public class DatabaseMapDictionaryHashed<T, U, TH> extends
		ResourceSupport<DatabaseStage<Object2ObjectSortedMap<T, U>>, DatabaseMapDictionaryHashed<T, U, TH>> implements
		DatabaseStageMap<T, U, DatabaseStageEntry<U>> {

	private static final Logger logger = LogManager.getLogger(DatabaseMapDictionaryHashed.class);

	private static final Drop<DatabaseMapDictionaryHashed<?, ?, ?>> DROP = new Drop<>() {
		@Override
		public void drop(DatabaseMapDictionaryHashed<?, ?, ?> obj) {
			try {
				if (obj.subDictionary != null) {
					obj.subDictionary.close();
				}
			} catch (Throwable ex) {
				logger.error("Failed to close subDictionary", ex);
			}
		}

		@Override
		public Drop<DatabaseMapDictionaryHashed<?, ?, ?>> fork() {
			return this;
		}

		@Override
		public void attach(DatabaseMapDictionaryHashed<?, ?, ?> obj) {

		}
	};

	private final BufferAllocator alloc;
	private final Function<T, TH> keySuffixHashFunction;

	private DatabaseMapDictionary<TH, ObjectArraySet<Entry<T, U>>> subDictionary;

	@SuppressWarnings({"unchecked", "rawtypes"})
	protected DatabaseMapDictionaryHashed(LLDictionary dictionary,
			@Nullable BufSupplier prefixKeySupplier,
			Serializer<T> keySuffixSerializer,
			Serializer<U> valueSerializer,
			Function<T, TH> keySuffixHashFunction,
			SerializerFixedBinaryLength<TH> keySuffixHashSerializer,
			Runnable onClose) {
		super((Drop<DatabaseMapDictionaryHashed<T, U, TH>>) (Drop) DROP);
		if (dictionary.getUpdateMode().transform(LLUtils::handleDiscard).block() != UpdateMode.ALLOW) {
			throw new IllegalArgumentException("Hashed maps only works when UpdateMode is ALLOW");
		}
		this.alloc = dictionary.getAllocator();
		ValueWithHashSerializer<T, U> valueWithHashSerializer
				= new ValueWithHashSerializer<>(keySuffixSerializer, valueSerializer);
		ValuesSetSerializer<Entry<T, U>> valuesSetSerializer
				= new ValuesSetSerializer<>(valueWithHashSerializer);
		this.subDictionary = DatabaseMapDictionary.tail(dictionary, prefixKeySupplier, keySuffixHashSerializer,
				valuesSetSerializer, onClose);
		this.keySuffixHashFunction = keySuffixHashFunction;
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	private DatabaseMapDictionaryHashed(BufferAllocator alloc,
			Function<T, TH> keySuffixHashFunction,
			Send<DatabaseStage<Object2ObjectSortedMap<TH, ObjectArraySet<Entry<T, U>>>>> subDictionary,
			Drop<DatabaseMapDictionaryHashed<T, U, TH>> drop) {
		super((Drop<DatabaseMapDictionaryHashed<T, U, TH>>) (Drop) DROP);
		this.alloc = alloc;
		this.keySuffixHashFunction = keySuffixHashFunction;

		this.subDictionary = (DatabaseMapDictionary<TH, ObjectArraySet<Entry<T, U>>>) subDictionary.receive();
	}

	public static <T, U, UH> DatabaseMapDictionaryHashed<T, U, UH> simple(LLDictionary dictionary,
			Serializer<T> keySerializer,
			Serializer<U> valueSerializer,
			Function<T, UH> keyHashFunction,
			SerializerFixedBinaryLength<UH> keyHashSerializer,
			Runnable onClose) {
		return new DatabaseMapDictionaryHashed<>(
				dictionary,
				null,
				keySerializer,
				valueSerializer,
				keyHashFunction,
				keyHashSerializer,
				onClose
		);
	}

	public static <T, U, UH> DatabaseMapDictionaryHashed<T, U, UH> tail(LLDictionary dictionary,
			@Nullable BufSupplier prefixKeySupplier,
			Serializer<T> keySuffixSerializer,
			Serializer<U> valueSerializer,
			Function<T, UH> keySuffixHashFunction,
			SerializerFixedBinaryLength<UH> keySuffixHashSerializer,
			Runnable onClose) {
		return new DatabaseMapDictionaryHashed<>(dictionary,
				prefixKeySupplier,
				keySuffixSerializer,
				valueSerializer,
				keySuffixHashFunction,
				keySuffixHashSerializer,
				onClose
		);
	}

	private Object2ObjectSortedMap<TH, ObjectArraySet<Entry<T, U>>> serializeMap(Object2ObjectSortedMap<T, U> map) {
		var newMap = new Object2ObjectLinkedOpenHashMap<TH, ObjectArraySet<Entry<T, U>>>(map.size());
		map.forEach((key, value) -> newMap.compute(keySuffixHashFunction.apply(key), (hash, prev) -> {
			if (prev == null) {
				prev = new ObjectArraySet<>();
			}
			prev.add(Map.entry(key, value));
			return prev;
		}));
		return newMap;
	}

	private Object2ObjectSortedMap<T, U> deserializeMap(Object2ObjectSortedMap<TH, ObjectArraySet<Entry<T, U>>> map) {
		var newMap = new Object2ObjectLinkedOpenHashMap<T, U>(map.size());
		map.forEach((hash, set) -> set.forEach(entry -> newMap.put(entry.getKey(), entry.getValue())));
		return newMap;
	}

	@Override
	public Mono<Object2ObjectSortedMap<T, U>> get(@Nullable CompositeSnapshot snapshot) {
		return subDictionary.get(snapshot).map(map -> deserializeMap(map));
	}

	@Override
	public Mono<Object2ObjectSortedMap<T, U>> getOrDefault(@Nullable CompositeSnapshot snapshot,
			Mono<Object2ObjectSortedMap<T, U>> defaultValue) {
		return this.get(snapshot).switchIfEmpty(defaultValue);
	}

	@Override
	public Mono<Void> set(Object2ObjectSortedMap<T, U> map) {
		return Mono.fromSupplier(() -> this.serializeMap(map)).flatMap(value -> subDictionary.set(value));
	}

	@Override
	public Mono<Boolean> setAndGetChanged(Object2ObjectSortedMap<T, U> map) {
		return Mono
				.fromSupplier(() -> this.serializeMap(map))
				.flatMap(value -> subDictionary.setAndGetChanged(value))
				.single();
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
	public DatabaseStageEntry<Object2ObjectSortedMap<T, U>> entry() {
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
				.map(entry -> new DatabaseSingleBucket<T, U, TH>(entry, key, null));
	}

	@Override
	public Mono<UpdateMode> getUpdateMode() {
		return subDictionary.getUpdateMode();
	}

	@Override
	public Flux<SubStageEntry<T, DatabaseStageEntry<U>>> getAllStages(@Nullable CompositeSnapshot snapshot,
			boolean smallRange) {
		return subDictionary
				.getAllValues(snapshot, smallRange)
				.map(Entry::getValue)
				.map(Collections::unmodifiableSet)
				.flatMap(bucket -> Flux
						.fromIterable(bucket)
						.map(Entry::getKey)
						.flatMap(key -> this.at(snapshot, key).map(stage -> new SubStageEntry<>(key, stage))));
	}

	@Override
	public Flux<Entry<T, U>> getAllValues(@Nullable CompositeSnapshot snapshot, boolean smallRange) {
		return subDictionary
				.getAllValues(snapshot, smallRange)
				.map(Entry::getValue)
				.map(Collections::unmodifiableSet)
				.concatMapIterable(list -> list);
	}

	@Override
	public Flux<Entry<T, U>> setAllValuesAndGetPrevious(Flux<Entry<T, U>> entries) {
		return entries.flatMap(entry -> Mono.usingWhen(this.at(null, entry.getKey()),
				stage -> stage.setAndGetPrevious(entry.getValue()).map(prev -> Map.entry(entry.getKey(), prev)),
				LLUtils::finalizeResource
		));
	}

	@Override
	public Mono<Void> clear() {
		return subDictionary.clear();
	}

	@Override
	public Mono<Object2ObjectSortedMap<T, U>> setAndGetPrevious(Object2ObjectSortedMap<T, U> value) {
		return Mono
				.fromSupplier(() -> this.serializeMap(value))
				.flatMap(value1 -> subDictionary.setAndGetPrevious(value1))
				.map(map -> deserializeMap(map));
	}

	@Override
	public Mono<Object2ObjectSortedMap<T, U>> clearAndGetPrevious() {
		return subDictionary
				.clearAndGetPrevious()
				.map(map -> deserializeMap(map));
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
}
