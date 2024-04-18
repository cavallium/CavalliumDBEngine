package it.cavallium.dbengine.database.collections;

import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.client.DbProgress;
import it.cavallium.dbengine.client.SSTVerificationProgress;
import it.cavallium.dbengine.database.Delta;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.UpdateReturnMode;
import it.cavallium.dbengine.database.serialization.SerializationFunction;
import it.unimi.dsi.fastutil.objects.ObjectArraySet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

@SuppressWarnings("unused")
public class DatabaseSingleBucket<K, V, TH> implements DatabaseStageEntry<V> {

	private static final Logger logger = LogManager.getLogger(DatabaseSingleBucket.class);

	private final K key;

	private final DatabaseStageEntry<ObjectArraySet<Entry<K, V>>> bucketStage;

	public DatabaseSingleBucket(DatabaseStageEntry<ObjectArraySet<Entry<K, V>>> bucketStage, K key) {
		this.key = key;
		this.bucketStage = bucketStage;
	}

	private DatabaseSingleBucket(DatabaseStage<ObjectArraySet<Entry<K, V>>> bucketStage, K key) {
		this.key = key;
		this.bucketStage = (DatabaseStageEntry<ObjectArraySet<Entry<K, V>>>) bucketStage;
	}

	@Override
	public ForkJoinPool getDbReadPool() {
		return bucketStage.getDbReadPool();
	}

	@Override
	public ForkJoinPool getDbWritePool() {
		return bucketStage.getDbWritePool();
	}

	@Override
	public V get(@Nullable CompositeSnapshot snapshot) {
		var entries = bucketStage.get(snapshot);
		return entries != null ? extractValue(entries) : null;
	}

	@Override
	public V getOrDefault(@Nullable CompositeSnapshot snapshot, V defaultValue) {
		var entries = bucketStage.get(snapshot);
		return entries != null ? extractValue(entries) : defaultValue;
	}

	@Override
	public void set(V value) {
		this.update(prev -> value, UpdateReturnMode.NOTHING);
	}

	@Override
	public V setAndGetPrevious(V value) {
		return this.update(prev -> value, UpdateReturnMode.GET_OLD_VALUE);
	}

	@Override
	public boolean setAndGetChanged(V value) {
		return LLUtils.isDeltaChanged(this.updateAndGetDelta(prev -> value));
	}

	@Override
	public V update(SerializationFunction<@Nullable V, @Nullable V> updater, UpdateReturnMode updateReturnMode) {
		var result = bucketStage
				.update(oldBucket -> {
					V oldValue = extractValue(oldBucket);
 					V newValue = updater.apply(oldValue);
					
					if (newValue == null) {
						return this.removeValueOrDelete(oldBucket);
					} else {
						return this.insertValueOrCreate(oldBucket, newValue);
					}
				}, updateReturnMode);
		return result != null ? extractValue(result) : null;
	}

	@Override
	public Delta<V> updateAndGetDelta(SerializationFunction<@Nullable V, @Nullable V> updater) {
		var delta = bucketStage.updateAndGetDelta(oldBucket -> {
			V oldValue = extractValue(oldBucket);
			var result = updater.apply(oldValue);
			if (result == null) {
				return this.removeValueOrDelete(oldBucket);
			} else {
				return this.insertValueOrCreate(oldBucket, result);
			}
		});
		return LLUtils.mapDelta(delta, this::extractValue);
	}

	@Override
	public void clear() {
		this.update(prev -> null, UpdateReturnMode.NOTHING);
	}

	@Override
	public V clearAndGetPrevious() {
		return this.update(prev -> null, UpdateReturnMode.GET_OLD_VALUE);
	}

	@Override
	public boolean clearAndGetStatus() {
		return LLUtils.isDeltaChanged(this.updateAndGetDelta(prev -> null));
	}

	@Override
	public long leavesCount(@Nullable CompositeSnapshot snapshot, boolean fast) {
		return this.get(snapshot) != null ? 1L : 0L;
	}

	@Override
	public boolean isEmpty(@Nullable CompositeSnapshot snapshot) {
		return this.get(snapshot) == null;
	}

	@Override
	public DatabaseStageEntry<V> entry() {
		return this;
	}

	@Override
	public Stream<DbProgress<SSTVerificationProgress>> verifyChecksum() {
		return bucketStage.verifyChecksum();
	}

	@Nullable
	private V extractValue(Set<Entry<K, V>> entries) {
		if (entries == null) return null;
		for (Entry<K, V> entry : entries) {
			if (Objects.equals(entry.getKey(), key)) {
				return entry.getValue();
			}
		}
		return null;
	}

	@NotNull
	private ObjectArraySet<Entry<K, V>> insertValueOrCreate(@Nullable ObjectArraySet<Entry<K, V>> entries, V value) {
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
			clonedEntries.add(Map.entry(key, value));
			return clonedEntries;
		} else {
			var oas = new ObjectArraySet<Entry<K, V>>(1);
			oas.add(Map.entry(key, value));
			return oas;
		}
	}

	@Nullable
	private ObjectArraySet<Entry<K, V>> removeValueOrDelete(@Nullable ObjectArraySet<Entry<K, V>> entries) {
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
