package it.cavallium.dbengine.database.collections;

import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.Delta;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.UpdateReturnMode;
import it.unimi.dsi.fastutil.objects.ObjectArraySet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.warp.commonutils.functional.TriFunction;
import reactor.core.publisher.Mono;

@SuppressWarnings("unused")
public class DatabaseSingleBucket<K, V, TH> implements DatabaseStageEntry<V> {

	private final DatabaseStageEntry<Set<Entry<K, V>>> bucketStage;
	private final K key;

	public DatabaseSingleBucket(DatabaseStageEntry<Set<Entry<K, V>>> bucketStage, K key) {
		this.bucketStage = bucketStage;
		this.key = key;
	}

	@Override
	public Mono<V> get(@Nullable CompositeSnapshot snapshot, boolean existsAlmostCertainly) {
		return bucketStage.get(snapshot, existsAlmostCertainly).flatMap(this::extractValueTransformation);
	}

	@Override
	public Mono<V> getOrDefault(@Nullable CompositeSnapshot snapshot, Mono<V> defaultValue) {
		return bucketStage.get(snapshot).flatMap(this::extractValueTransformation).switchIfEmpty(defaultValue);
	}

	@Override
	public Mono<Void> set(V value) {
		return this.update(prev -> value, UpdateReturnMode.NOTHING).then();
	}

	@Override
	public Mono<V> setAndGetPrevious(V value) {
		return this.update(prev -> value, UpdateReturnMode.GET_OLD_VALUE);
	}

	@Override
	public Mono<Boolean> setAndGetChanged(V value) {
		return this.updateAndGetDelta(prev -> value).map(LLUtils::isDeltaChanged);
	}

	@Override
	public Mono<V> update(Function<@Nullable V, @Nullable V> updater,
			UpdateReturnMode updateReturnMode,
			boolean existsAlmostCertainly) {
		return bucketStage
				.update(oldBucket -> {
					V oldValue = extractValue(oldBucket);
					V newValue = updater.apply(oldValue);
					
					if (newValue == null) {
						return this.removeValueOrDelete(oldBucket);
					} else {
						return this.insertValueOrCreate(oldBucket, newValue);
					}
				}, updateReturnMode, existsAlmostCertainly)
				.flatMap(this::extractValueTransformation);
	}

	@Override
	public Mono<Delta<V>> updateAndGetDelta(Function<@Nullable V, @Nullable V> updater, boolean existsAlmostCertainly) {
		return bucketStage
				.updateAndGetDelta(oldBucket -> {
					V oldValue = extractValue(oldBucket);
					var result = updater.apply(oldValue);
					if (result == null) {
						return this.removeValueOrDelete(oldBucket);
					} else {
						return this.insertValueOrCreate(oldBucket, result);
					}
				}, existsAlmostCertainly)
				.transform(mono -> LLUtils.mapDelta(mono, this::extractValue));
	}

	@Override
	public Mono<Void> clear() {
		return this.update(prev -> null, UpdateReturnMode.NOTHING).then();
	}

	@Override
	public Mono<V> clearAndGetPrevious() {
		return this.update(prev -> null, UpdateReturnMode.GET_OLD_VALUE);
	}

	@Override
	public Mono<Boolean> clearAndGetStatus() {
		return this.updateAndGetDelta(prev -> null).map(LLUtils::isDeltaChanged);
	}

	@Override
	public Mono<Void> close() {
		return bucketStage.close();
	}

	@Override
	public Mono<Long> leavesCount(@Nullable CompositeSnapshot snapshot, boolean fast) {
		return this.get(snapshot).map(prev -> 1L).defaultIfEmpty(0L);
	}

	@Override
	public Mono<Boolean> isEmpty(@Nullable CompositeSnapshot snapshot) {
		return this.get(snapshot).map(prev -> true).defaultIfEmpty(true);
	}

	@Override
	public DatabaseStageEntry<V> entry() {
		return this;
	}

	@Override
	public void release() {
		bucketStage.release();
	}

	private Mono<V> extractValueTransformation(Set<Entry<K, V>> entries) {
		return Mono.fromCallable(() -> extractValue(entries));
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
	private Set<Entry<K, V>> insertValueOrCreate(@Nullable Set<Entry<K, V>> entries, V value) {
		if (entries != null) {
			var it = entries.iterator();
			while (it.hasNext()) {
				var entry = it.next();
				if (Objects.equals(entry.getKey(), key)) {
					it.remove();
					break;
				}
			}
			entries.add(Map.entry(key, value));
			return entries;
		} else {
			var oas = new ObjectArraySet<Entry<K, V>>(1);
			oas.add(Map.entry(key, value));
			return oas;
		}
	}

	@Nullable
	private Set<Entry<K, V>> removeValueOrDelete(@Nullable Set<Entry<K, V>> entries) {
		if (entries != null) {
			var it = entries.iterator();
			while (it.hasNext()) {
				var entry = it.next();
				if (Objects.equals(entry.getKey(), key)) {
					it.remove();
					break;
				}
			}
			if (entries.size() == 0) {
				return null;
			} else {
				return entries;
			}
		} else {
			return null;
		}
	}
}
