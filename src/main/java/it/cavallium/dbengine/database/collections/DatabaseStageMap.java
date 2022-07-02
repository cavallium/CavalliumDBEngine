package it.cavallium.dbengine.database.collections;

import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.Delta;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.SubStageEntry;
import it.cavallium.dbengine.database.UpdateMode;
import it.cavallium.dbengine.database.UpdateReturnMode;
import it.cavallium.dbengine.database.serialization.KVSerializationFunction;
import it.cavallium.dbengine.database.serialization.SerializationException;
import it.cavallium.dbengine.database.serialization.SerializationFunction;
import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectSortedMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectSortedMaps;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

@SuppressWarnings("unused")
public interface DatabaseStageMap<T, U, US extends DatabaseStage<U>> extends
		DatabaseStageEntry<Object2ObjectSortedMap<T, U>> {

	Mono<US> at(@Nullable CompositeSnapshot snapshot, T key);

	default Mono<Boolean> containsKey(@Nullable CompositeSnapshot snapshot, T key) {
		return Mono.usingWhen(this.at(snapshot, key),
				stage -> stage.isEmpty(snapshot).map(empty -> !empty),
				LLUtils::finalizeResource
		);
	}

	default Mono<U> getValue(@Nullable CompositeSnapshot snapshot, T key) {
		return Mono.usingWhen(this.at(snapshot, key),
				stage -> stage.get(snapshot),
				LLUtils::finalizeResource
		);
	}

	default Mono<U> getValueOrDefault(@Nullable CompositeSnapshot snapshot, T key, Mono<U> defaultValue) {
		return getValue(snapshot, key).switchIfEmpty(defaultValue).single();
	}

	default Mono<Void> putValue(T key, U value) {
		return Mono.usingWhen(at(null, key).single(), stage -> stage.set(value), LLUtils::finalizeResource);
	}

	Mono<UpdateMode> getUpdateMode();

	default Mono<U> updateValue(T key,
			UpdateReturnMode updateReturnMode,
			SerializationFunction<@Nullable U, @Nullable U> updater) {
		return Mono.usingWhen(at(null, key).single(),
				stage -> stage.update(updater, updateReturnMode),
				LLUtils::finalizeResource
		);
	}

	default Flux<Boolean> updateMulti(Flux<T> keys, KVSerializationFunction<T, @Nullable U, @Nullable U> updater) {
		return keys.flatMapSequential(key -> this.updateValue(key, prevValue -> updater.apply(key, prevValue)));
	}

	default Mono<Boolean> updateValue(T key, SerializationFunction<@Nullable U, @Nullable U> updater) {
		return updateValueAndGetDelta(key, updater).map(delta -> LLUtils.isDeltaChanged(delta)).single();
	}

	default Mono<Delta<U>> updateValueAndGetDelta(T key,
			SerializationFunction<@Nullable U, @Nullable U> updater) {
		var stageMono = this.at(null, key).single();
		return stageMono.flatMap(stage -> stage
				.updateAndGetDelta(updater)
				.doFinally(s -> stage.close()));
	}

	default Mono<U> putValueAndGetPrevious(T key, U value) {
		return Mono.usingWhen(at(null, key).single(),
				stage -> stage.setAndGetPrevious(value),
				LLUtils::finalizeResource
		);
	}

	/**
	 * @return true if the key was associated with any value, false if the key didn't exist.
	 */
	default Mono<Boolean> putValueAndGetChanged(T key, U value) {
		return Mono
				.usingWhen(at(null, key).single(), stage -> stage.setAndGetChanged(value), LLUtils::finalizeResource)
				.single();
	}

	default Mono<Void> remove(T key) {
		return removeAndGetStatus(key).then();
	}

	default Mono<U> removeAndGetPrevious(T key) {
		return Mono.usingWhen(at(null, key), us -> us.clearAndGetPrevious(), LLUtils::finalizeResource);
	}

	default Mono<Boolean> removeAndGetStatus(T key) {
		return removeAndGetPrevious(key).map(o -> true).defaultIfEmpty(false);
	}

	/**
	 * GetMulti must return the elements in sequence!
	 */
	default Flux<Optional<U>> getMulti(@Nullable CompositeSnapshot snapshot, Flux<T> keys) {
		return keys.flatMapSequential(key -> this
				.getValue(snapshot, key)
				.map(Optional::of)
				.defaultIfEmpty(Optional.empty())
		);
	}

	default Mono<Void> putMulti(Flux<Entry<T, U>> entries) {
		return entries.flatMap(entry -> this.putValue(entry.getKey(), entry.getValue())).then();
	}

	Flux<SubStageEntry<T, US>> getAllStages(@Nullable CompositeSnapshot snapshot, boolean smallRange);

	default Flux<Entry<T, U>> getAllValues(@Nullable CompositeSnapshot snapshot, boolean smallRange) {
		return this
				.getAllStages(snapshot, smallRange)
				.flatMapSequential(stage -> stage
						.getValue()
						.get(snapshot)
						.map(value -> Map.entry(stage.getKey(), value))
						.doFinally(s -> stage.getValue().close())
				);
	}

	default Mono<Void> setAllValues(Flux<Entry<T, U>> entries) {
		return setAllValuesAndGetPrevious(entries).then();
	}

	Flux<Entry<T, U>> setAllValuesAndGetPrevious(Flux<Entry<T, U>> entries);

	default Mono<Void> clear() {
		return setAllValues(Flux.empty());
	}

	default Mono<Void> replaceAllValues(boolean canKeysChange,
			Function<Entry<T, U>, Mono<Entry<T, U>>> entriesReplacer,
			boolean smallRange) {
		if (canKeysChange) {
			return this.setAllValues(this.getAllValues(null, smallRange).flatMap(entriesReplacer)).then();
		} else {
			return this
					.getAllValues(null, smallRange)
					.flatMap(entriesReplacer)
					.flatMap(replacedEntry -> this
							.at(null, replacedEntry.getKey())
							.flatMap(stage -> stage
									.set(replacedEntry.getValue())
									.doFinally(s -> stage.close())
							)
					)
					.then();
		}
	}

	default Mono<Void> replaceAll(Function<Entry<T, US>, Mono<Void>> entriesReplacer) {
		return this
				.getAllStages(null, false)
				.flatMap(stage -> entriesReplacer.apply(stage)
						.doFinally(s -> stage.getValue().close())
				)
				.then();
	}

	@Override
	default Mono<Object2ObjectSortedMap<T, U>> setAndGetPrevious(Object2ObjectSortedMap<T, U> value) {
		return this
				.setAllValuesAndGetPrevious(Flux.fromIterable(value.entrySet()))
				.collectMap(Entry::getKey, Entry::getValue, Object2ObjectLinkedOpenHashMap::new)
				.map(map -> (Object2ObjectSortedMap<T, U>) map)
				.filter(map -> !map.isEmpty());
	}

	@Override
	default Mono<Boolean> setAndGetChanged(Object2ObjectSortedMap<T, U> value) {
		return this
				.setAndGetPrevious(value)
				.map(oldValue -> !Objects.equals(oldValue, value.isEmpty() ? null : value))
				.switchIfEmpty(Mono.fromSupplier(() -> !value.isEmpty()));
	}

	@Override
	default Mono<Delta<Object2ObjectSortedMap<T, U>>> updateAndGetDelta(SerializationFunction<@Nullable Object2ObjectSortedMap<T, U>, @Nullable Object2ObjectSortedMap<T, U>> updater) {
		return this
				.getUpdateMode()
				.single()
				.flatMap(updateMode -> {
					if (updateMode == UpdateMode.ALLOW_UNSAFE) {
						return this
								.getAllValues(null, true)
								.collectMap(Entry::getKey, Entry::getValue, Object2ObjectLinkedOpenHashMap::new)
								.map(map -> (Object2ObjectSortedMap<T, U>) map)
								.single()
								.<Tuple2<Optional<Object2ObjectSortedMap<T, U>>, Optional<Object2ObjectSortedMap<T, U>>>>handle((v, sink) -> {
									if (v.isEmpty()) {
										v = null;
									}
									try {
										var result = updater.apply(v);
										if (result != null && result.isEmpty()) {
											result = null;
										}
										sink.next(Tuples.of(Optional.ofNullable(v), Optional.ofNullable(result)));
									} catch (SerializationException ex) {
										sink.error(ex);
									}
								})
								.flatMap(result -> Mono
										.justOrEmpty(result.getT2())
										.flatMap(values -> this.setAllValues(Flux.fromIterable(values.entrySet())))
										.thenReturn(new Delta<>(result.getT1().orElse(null), result.getT2().orElse(null)))
								);
					} else if (updateMode == UpdateMode.ALLOW) {
						return Mono.fromCallable(() -> {
							throw new UnsupportedOperationException("Maps can't be updated atomically");
						});
					} else if (updateMode == UpdateMode.DISALLOW) {
						return Mono.fromCallable(() -> {
							throw new UnsupportedOperationException("Map can't be updated because updates are disabled");
						});
					} else {
						return Mono.fromCallable(() -> {
							throw new UnsupportedOperationException("Unknown update mode: " + updateMode);
						});
					}
				});
	}

	@Override
	default Mono<Object2ObjectSortedMap<T, U>> clearAndGetPrevious() {
		return this.setAndGetPrevious(Object2ObjectSortedMaps.emptyMap());
	}

	@Override
	default Mono<Object2ObjectSortedMap<T, U>> get(@Nullable CompositeSnapshot snapshot) {
		return this
				.getAllValues(snapshot, true)
				.collectMap(Entry::getKey, Entry::getValue, Object2ObjectLinkedOpenHashMap::new)
				.map(map -> (Object2ObjectSortedMap<T, U>) map)
				.filter(map -> !map.isEmpty());
	}

	@Override
	default Mono<Long> leavesCount(@Nullable CompositeSnapshot snapshot, boolean fast) {
		return this
				.getAllStages(snapshot, false)
				.doOnNext(stage -> stage.getValue().close())
				.count();
	}

	/**
	 * Value getter doesn't lock data. Please make sure to lock before getting data.
	 */
	default ValueGetterBlocking<T, U> getDbValueGetter(@Nullable CompositeSnapshot snapshot) {
		return k -> getValue(snapshot, k).transform(LLUtils::handleDiscard).block();
	}

	default ValueGetter<T, U> getAsyncDbValueGetter(@Nullable CompositeSnapshot snapshot) {
		return k -> getValue(snapshot, k);
	}

	default ValueTransformer<T, U> getAsyncDbValueTransformer(@Nullable CompositeSnapshot snapshot) {
		return keys -> {
			var sharedKeys = keys.publish().refCount(2);
			var values = DatabaseStageMap.this.getMulti(snapshot, sharedKeys);
			return Flux.zip(sharedKeys, values, Map::entry);
		};
	}
}
