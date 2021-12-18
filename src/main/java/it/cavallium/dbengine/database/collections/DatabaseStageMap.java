package it.cavallium.dbengine.database.collections;

import io.net5.buffer.api.Buffer;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.Delta;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.UpdateMode;
import it.cavallium.dbengine.database.UpdateReturnMode;
import it.cavallium.dbengine.database.serialization.KVSerializationFunction;
import it.cavallium.dbengine.database.serialization.SerializationException;
import it.cavallium.dbengine.database.serialization.SerializationFunction;
import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectSortedMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectSortedMaps;
import java.util.HashMap;
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

	default Mono<U> getValue(@Nullable CompositeSnapshot snapshot, T key, boolean existsAlmostCertainly) {
		return LLUtils.usingResource(this.at(snapshot, key),
				stage -> stage.get(snapshot, existsAlmostCertainly), true);
	}

	default Mono<U> getValue(@Nullable CompositeSnapshot snapshot, T key) {
		return getValue(snapshot, key, false);
	}

	default Mono<U> getValueOrDefault(@Nullable CompositeSnapshot snapshot, T key, Mono<U> defaultValue) {
		return getValue(snapshot, key).switchIfEmpty(defaultValue).single();
	}

	default Mono<Void> putValue(T key, U value) {
		return LLUtils.usingResource(at(null, key).single(),
				stage -> stage.set(value), true);
	}

	Mono<UpdateMode> getUpdateMode();

	default Mono<U> updateValue(T key,
			UpdateReturnMode updateReturnMode,
			boolean existsAlmostCertainly,
			SerializationFunction<@Nullable U, @Nullable U> updater) {
		return LLUtils.usingResource(this.at(null, key).single(),
				stage -> stage.update(updater, updateReturnMode, existsAlmostCertainly), true);
	}

	default Flux<Boolean> updateMulti(Flux<T> keys, KVSerializationFunction<T, @Nullable U, @Nullable U> updater) {
		return keys.flatMapSequential(key -> this.updateValue(key, prevValue -> updater.apply(key, prevValue)));
	}

	default Mono<U> updateValue(T key, UpdateReturnMode updateReturnMode, SerializationFunction<@Nullable U, @Nullable U> updater) {
		return updateValue(key, updateReturnMode, false, updater);
	}

	default Mono<Boolean> updateValue(T key, SerializationFunction<@Nullable U, @Nullable U> updater) {
		return updateValueAndGetDelta(key, false, updater).map(LLUtils::isDeltaChanged).single();
	}

	default Mono<Boolean> updateValue(T key, boolean existsAlmostCertainly, SerializationFunction<@Nullable U, @Nullable U> updater) {
		return updateValueAndGetDelta(key, existsAlmostCertainly, updater).map(LLUtils::isDeltaChanged).single();
	}

	default Mono<Delta<U>> updateValueAndGetDelta(T key,
			boolean existsAlmostCertainly,
			SerializationFunction<@Nullable U, @Nullable U> updater) {
		return LLUtils.usingResource(this.at(null, key).single(),
				stage -> stage.updateAndGetDelta(updater, existsAlmostCertainly), true);
	}

	default Mono<Delta<U>> updateValueAndGetDelta(T key, SerializationFunction<@Nullable U, @Nullable U> updater) {
		return updateValueAndGetDelta(key, false, updater);
	}

	default Mono<U> putValueAndGetPrevious(T key, U value) {
		return LLUtils.usingResource(at(null, key).single(), stage -> stage.setAndGetPrevious(value), true);
	}

	/**
	 * @return true if the key was associated with any value, false if the key didn't exist.
	 */
	default Mono<Boolean> putValueAndGetChanged(T key, U value) {
		return LLUtils.usingResource(at(null, key).single(), stage -> stage.setAndGetChanged(value), true).single();
	}

	default Mono<Void> remove(T key) {
		return removeAndGetStatus(key).then();
	}

	default Mono<U> removeAndGetPrevious(T key) {
		return LLUtils.usingResource(at(null, key), DatabaseStage::clearAndGetPrevious, true);
	}

	default Mono<Boolean> removeAndGetStatus(T key) {
		return removeAndGetPrevious(key).map(o -> true).defaultIfEmpty(false);
	}

	/**
	 * GetMulti must return the elements in sequence!
	 */
	default Flux<Optional<U>> getMulti(@Nullable CompositeSnapshot snapshot, Flux<T> keys, boolean existsAlmostCertainly) {
		return keys
				.flatMapSequential(key -> this.getValue(snapshot, key, existsAlmostCertainly))
				.map(Optional::of)
				.defaultIfEmpty(Optional.empty())
				.doOnDiscard(Entry.class, unknownEntry -> {
					if (unknownEntry.getValue() instanceof Optional optionalBuffer
							&& optionalBuffer.isPresent()
							&& optionalBuffer.get() instanceof Buffer buffer) {
						buffer.close();
					}
				});
	}

	/**
	 * GetMulti must return the elements in sequence!
	 */
	default Flux<Optional<U>> getMulti(@Nullable CompositeSnapshot snapshot, Flux<T> keys) {
		return getMulti(snapshot, keys, false);
	}

	default Mono<Void> putMulti(Flux<Entry<T, U>> entries) {
		return entries.flatMap(entry -> this.putValue(entry.getKey(), entry.getValue())).then();
	}

	Flux<Entry<T, US>> getAllStages(@Nullable CompositeSnapshot snapshot);

	default Flux<Entry<T, U>> getAllValues(@Nullable CompositeSnapshot snapshot) {
		return this
				.getAllStages(snapshot)
				.flatMapSequential(stage -> stage
						.getValue()
						.get(snapshot, true)
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

	default Mono<Void> replaceAllValues(boolean canKeysChange, Function<Entry<T, U>,
			Mono<Entry<T, U>>> entriesReplacer) {
		if (canKeysChange) {
			return this.setAllValues(this.getAllValues(null).flatMap(entriesReplacer)).then();
		} else {
			return this
					.getAllValues(null)
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
				.getAllStages(null)
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
	default Mono<Delta<Object2ObjectSortedMap<T, U>>> updateAndGetDelta(SerializationFunction<@Nullable Object2ObjectSortedMap<T, U>, @Nullable Object2ObjectSortedMap<T, U>> updater,
			boolean existsAlmostCertainly) {
		return this
				.getUpdateMode()
				.single()
				.flatMap(updateMode -> {
					if (updateMode == UpdateMode.ALLOW_UNSAFE) {
						return this
								.getAllValues(null)
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
	default Mono<Object2ObjectSortedMap<T, U>> get(@Nullable CompositeSnapshot snapshot, boolean existsAlmostCertainly) {
		return this
				.getAllValues(snapshot)
				.collectMap(Entry::getKey, Entry::getValue, Object2ObjectLinkedOpenHashMap::new)
				.map(map -> (Object2ObjectSortedMap<T, U>) map)
				.filter(map -> !map.isEmpty());
	}

	@Override
	default Mono<Long> leavesCount(@Nullable CompositeSnapshot snapshot, boolean fast) {
		return this
				.getAllStages(snapshot)
				.doOnNext(stage -> stage.getValue().close())
				.count();
	}

	/**
	 * Value getter doesn't lock data. Please make sure to lock before getting data.
	 */
	default ValueGetterBlocking<T, U> getDbValueGetter(@Nullable CompositeSnapshot snapshot) {
		return k -> getValue(snapshot, k).block();
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
