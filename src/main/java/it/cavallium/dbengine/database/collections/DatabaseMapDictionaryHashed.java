package it.cavallium.dbengine.database.collections;

import it.cavallium.buffer.Buf;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.client.DbProgress;
import it.cavallium.dbengine.client.SSTVerificationProgress;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.SubStageEntry;
import it.cavallium.dbengine.database.UpdateMode;
import it.cavallium.dbengine.database.serialization.Serializer;
import it.cavallium.dbengine.database.serialization.SerializerFixedBinaryLength;
import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectSortedMap;
import it.unimi.dsi.fastutil.objects.ObjectArraySet;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

@SuppressWarnings("unused")
public class DatabaseMapDictionaryHashed<T, U, TH> implements DatabaseStageMap<T, U, DatabaseStageEntry<U>> {

	private static final Logger logger = LogManager.getLogger(DatabaseMapDictionaryHashed.class);

	private final Function<T, TH> keySuffixHashFunction;

	private final DatabaseMapDictionary<TH, ObjectArraySet<Entry<T, U>>> subDictionary;

	protected DatabaseMapDictionaryHashed(LLDictionary dictionary,
			@Nullable Buf prefixKeySupplier,
			Serializer<T> keySuffixSerializer,
			Serializer<U> valueSerializer,
			Function<T, TH> keySuffixHashFunction,
			SerializerFixedBinaryLength<TH> keySuffixHashSerializer) {
		var updateMode = dictionary.getUpdateMode();
		if (updateMode != UpdateMode.ALLOW) {
			throw new IllegalArgumentException("Hashed maps only works when UpdateMode is ALLOW");
		}
		ValueWithHashSerializer<T, U> valueWithHashSerializer
				= new ValueWithHashSerializer<>(keySuffixSerializer, valueSerializer);
		ValuesSetSerializer<Entry<T, U>> valuesSetSerializer
				= new ValuesSetSerializer<>(valueWithHashSerializer);
		this.subDictionary = DatabaseMapDictionary.tail(dictionary, prefixKeySupplier, keySuffixHashSerializer,
				valuesSetSerializer);
		this.keySuffixHashFunction = keySuffixHashFunction;
	}

	private DatabaseMapDictionaryHashed(Function<T, TH> keySuffixHashFunction,
			DatabaseStage<Object2ObjectSortedMap<TH, ObjectArraySet<Entry<T, U>>>> subDictionary) {
		this.keySuffixHashFunction = keySuffixHashFunction;

		this.subDictionary = (DatabaseMapDictionary<TH, ObjectArraySet<Entry<T, U>>>) subDictionary;
	}

	public static <T, U, UH> DatabaseMapDictionaryHashed<T, U, UH> simple(LLDictionary dictionary,
			Serializer<T> keySerializer,
			Serializer<U> valueSerializer,
			Function<T, UH> keyHashFunction,
			SerializerFixedBinaryLength<UH> keyHashSerializer) {
		return new DatabaseMapDictionaryHashed<>(
				dictionary,
				null,
				keySerializer,
				valueSerializer,
				keyHashFunction,
				keyHashSerializer
		);
	}

	public static <T, U, UH> DatabaseMapDictionaryHashed<T, U, UH> tail(LLDictionary dictionary,
			@Nullable Buf prefixKeySupplier,
			Serializer<T> keySuffixSerializer,
			Serializer<U> valueSerializer,
			Function<T, UH> keySuffixHashFunction,
			SerializerFixedBinaryLength<UH> keySuffixHashSerializer) {
		return new DatabaseMapDictionaryHashed<>(dictionary,
				prefixKeySupplier,
				keySuffixSerializer,
				valueSerializer,
				keySuffixHashFunction,
				keySuffixHashSerializer
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
	public ForkJoinPool getDbReadPool() {
		return subDictionary.getDbReadPool();
	}

	@Override
	public ForkJoinPool getDbWritePool() {
		return subDictionary.getDbWritePool();
	}

	@Override
	public Object2ObjectSortedMap<T, U> get(@Nullable CompositeSnapshot snapshot) {
		var v = subDictionary.get(snapshot);
		var result = v != null ? deserializeMap(v) : null;
		return result != null && result.isEmpty() ? null : result;
	}

	@Override
	public Object2ObjectSortedMap<T, U> getOrDefault(@Nullable CompositeSnapshot snapshot,
			Object2ObjectSortedMap<T, U> defaultValue) {
		return Objects.requireNonNullElse(this.get(snapshot), defaultValue);
	}

	@Override
	public void set(Object2ObjectSortedMap<T, U> map) {
		var value = this.serializeMap(map);
		subDictionary.set(value);
	}

	@Override
	public boolean setAndGetChanged(Object2ObjectSortedMap<T, U> map) {
		return subDictionary.setAndGetChanged(this.serializeMap(map));
	}

	@Override
	public boolean clearAndGetStatus() {
		return subDictionary.clearAndGetStatus();
	}

	@Override
	public boolean isEmpty(@Nullable CompositeSnapshot snapshot) {
		return subDictionary.isEmpty(snapshot);
	}

	@Override
	public DatabaseStageEntry<Object2ObjectSortedMap<T, U>> entry() {
		return this;
	}

	@Override
	public Stream<DbProgress<SSTVerificationProgress>> verifyChecksum() {
		return this.subDictionary.verifyChecksum();
	}

	@Override
	public @NotNull DatabaseStageEntry<U> at(@Nullable CompositeSnapshot snapshot, T key) {
		return this.atPrivate(snapshot, key, keySuffixHashFunction.apply(key));
	}

	private DatabaseSingleBucket<T, U, TH> atPrivate(@Nullable CompositeSnapshot snapshot, T key, TH hash) {
		return new DatabaseSingleBucket<T, U, TH>(subDictionary.at(snapshot, hash), key);
	}

	@Override
	public UpdateMode getUpdateMode() {
		return subDictionary.getUpdateMode();
	}

	@Override
	public Stream<SubStageEntry<T, DatabaseStageEntry<U>>> getAllStages(@Nullable CompositeSnapshot snapshot,
			boolean smallRange) {
		return subDictionary
				.getAllEntries(snapshot, smallRange)
				.map(Entry::getValue)
				.map(Collections::unmodifiableSet)
				.flatMap(bucket -> bucket.stream()
						.map(Entry::getKey)
						.map(key -> new SubStageEntry<>(key, this.at(snapshot, key))));
	}

	@Override
	public Stream<Entry<T, U>> getAllEntries(@Nullable CompositeSnapshot snapshot, boolean smallRange) {
		return subDictionary
				.getAllEntries(snapshot, smallRange)
				.map(Entry::getValue)
				.map(Collections::unmodifiableSet)
				.flatMap(Collection::stream);
	}

	@Override
	public Stream<T> getAllKeys(@Nullable CompositeSnapshot snapshot, boolean smallRange) {
		return getAllEntries(snapshot, smallRange).map(Entry::getKey);
	}

	@Override
	public Stream<U> getAllValues(@Nullable CompositeSnapshot snapshot, boolean smallRange) {
		return getAllEntries(snapshot, smallRange).map(Entry::getValue);
	}

	@Override
	public Stream<Entry<T, U>> setAllEntriesAndGetPrevious(Stream<Entry<T, U>> entries) {
		List<Entry<T, U>> prevList = entries.map(entry -> {
			var prev = this.at(null, entry.getKey()).setAndGetPrevious(entry.getValue());
			if (prev != null) {
				return Map.entry(entry.getKey(), prev);
			} else {
				return null;
			}
		}).filter(Objects::nonNull).toList();
		return prevList.stream();
	}

	@Override
	public void clear() {
		subDictionary.clear();
	}

	@Override
	public Object2ObjectSortedMap<T, U> setAndGetPrevious(Object2ObjectSortedMap<T, U> value) {
		var v = subDictionary.setAndGetPrevious(this.serializeMap(value));
		var result = v != null ? deserializeMap(v) : null;
		return result != null && result.isEmpty() ? null : result;
	}

	@Override
	public Object2ObjectSortedMap<T, U> clearAndGetPrevious() {
		var v = subDictionary.clearAndGetPrevious();
		return v != null ? deserializeMap(v) : null;
	}

	@Override
	public long leavesCount(@Nullable CompositeSnapshot snapshot, boolean fast) {
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
		return key -> {
			ObjectArraySet<Entry<T, U>> set = getter.get(keySuffixHashFunction.apply(key));
			if (set != null) {
				return this.extractValue(set, key);
			} else {
				return null;
			}
		};
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
