package it.cavallium.dbengine.database.collections;

import io.net5.buffer.api.Drop;
import io.net5.buffer.api.Owned;
import io.net5.buffer.api.Send;
import it.cavallium.dbengine.client.BadBlock;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.Column;
import it.cavallium.dbengine.database.Delta;
import it.cavallium.dbengine.database.LLEntry;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.LiveResourceSupport;
import it.cavallium.dbengine.database.UpdateReturnMode;
import it.cavallium.dbengine.database.serialization.SerializationFunction;
import it.unimi.dsi.fastutil.objects.ObjectArraySet;
import it.unimi.dsi.fastutil.objects.ObjectSets;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.warp.commonutils.functional.TriFunction;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@SuppressWarnings("unused")
public class DatabaseSingleBucket<K, V, TH>
		extends LiveResourceSupport<DatabaseStage<V>, DatabaseSingleBucket<K, V, TH>>
		implements DatabaseStageEntry<V> {

	private final K key;

	private DatabaseStageEntry<ObjectArraySet<Entry<K, V>>> bucketStage;

	public DatabaseSingleBucket(DatabaseStageEntry<ObjectArraySet<Entry<K, V>>> bucketStage, K key,
			Drop<DatabaseSingleBucket<K, V, TH>> drop) {
		super(new CloseOnDrop<>(drop));
		this.key = key;
		this.bucketStage = bucketStage;
	}

	private DatabaseSingleBucket(Send<DatabaseStage<ObjectArraySet<Entry<K, V>>>> bucketStage, K key,
			Drop<DatabaseSingleBucket<K, V, TH>> drop) {
		super(new CloseOnDrop<>(drop));
		this.key = key;
		this.bucketStage = (DatabaseStageEntry<ObjectArraySet<Entry<K, V>>>) bucketStage.receive();
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
	public Mono<V> update(SerializationFunction<@Nullable V, @Nullable V> updater,
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
	public Mono<Delta<V>> updateAndGetDelta(SerializationFunction<@Nullable V, @Nullable V> updater,
			boolean existsAlmostCertainly) {
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
	protected RuntimeException createResourceClosedException() {
		throw new IllegalStateException("Closed");
	}

	@Override
	protected Owned<DatabaseSingleBucket<K, V, TH>> prepareSend() {
		var bucketStage = this.bucketStage.send();
		return drop -> new DatabaseSingleBucket<>(bucketStage, key, drop);
	}

	@Override
	protected void makeInaccessible() {
		this.bucketStage = null;
	}

	private static class CloseOnDrop<K, V, TH> implements
			Drop<DatabaseSingleBucket<K, V, TH>> {

		private final Drop<DatabaseSingleBucket<K, V, TH>> delegate;

		public CloseOnDrop(Drop<DatabaseSingleBucket<K, V, TH>> drop) {
			if (drop instanceof CloseOnDrop<K, V, TH> closeOnDrop) {
				this.delegate = closeOnDrop.delegate;
			} else {
				this.delegate = drop;
			}
		}

		@Override
		public void drop(DatabaseSingleBucket<K, V, TH> obj) {
			obj.bucketStage.close();
			delegate.drop(obj);
		}
	}
}
