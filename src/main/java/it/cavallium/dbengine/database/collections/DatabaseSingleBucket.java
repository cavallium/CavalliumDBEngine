package it.cavallium.dbengine.database.collections;

import io.netty5.buffer.Drop;
import io.netty5.buffer.Owned;
import io.netty5.util.Send;
import it.cavallium.dbengine.client.BadBlock;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.Delta;
import it.cavallium.dbengine.database.LLUtils;
import io.netty5.buffer.internal.ResourceSupport;
import it.cavallium.dbengine.database.UpdateReturnMode;
import it.cavallium.dbengine.database.serialization.SerializationFunction;
import it.cavallium.dbengine.utils.SimpleResource;
import it.unimi.dsi.fastutil.objects.ObjectArraySet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@SuppressWarnings("unused")
public class DatabaseSingleBucket<K, V, TH> extends SimpleResource implements DatabaseStageEntry<V> {

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
	public Mono<V> get(@Nullable CompositeSnapshot snapshot) {
		return bucketStage.get(snapshot).flatMap(entries -> extractValueTransformation(entries));
	}

	@Override
	public Mono<V> getOrDefault(@Nullable CompositeSnapshot snapshot, Mono<V> defaultValue) {
		return bucketStage.get(snapshot).flatMap(entries -> extractValueTransformation(entries)).switchIfEmpty(defaultValue);
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
		return this.updateAndGetDelta(prev -> value).map(delta -> LLUtils.isDeltaChanged(delta));
	}

	@Override
	public Mono<V> update(SerializationFunction<@Nullable V, @Nullable V> updater, UpdateReturnMode updateReturnMode) {
		return bucketStage
				.update(oldBucket -> {
					V oldValue = extractValue(oldBucket);
 					V newValue = updater.apply(oldValue);
					
					if (newValue == null) {
						return this.removeValueOrDelete(oldBucket);
					} else {
						return this.insertValueOrCreate(oldBucket, newValue);
					}
				}, updateReturnMode)
				.flatMap(entries -> extractValueTransformation(entries));
	}

	@Override
	public Mono<Delta<V>> updateAndGetDelta(SerializationFunction<@Nullable V, @Nullable V> updater) {
		return bucketStage.updateAndGetDelta(oldBucket -> {
			V oldValue = extractValue(oldBucket);
			var result = updater.apply(oldValue);
			if (result == null) {
				return this.removeValueOrDelete(oldBucket);
			} else {
				return this.insertValueOrCreate(oldBucket, result);
			}
		}).transform(mono -> LLUtils.mapDelta(mono, entries -> extractValue(entries)));
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
		return this.updateAndGetDelta(prev -> null).map(delta -> LLUtils.isDeltaChanged(delta));
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
	public Flux<BadBlock> badBlocks() {
		return bucketStage.badBlocks();
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

	@Override
	protected void onClose() {
		try {
			if (bucketStage != null) {
				bucketStage.close();
			}
		} catch (Throwable ex) {
			logger.error("Failed to close bucketStage", ex);
		}
	}
}
