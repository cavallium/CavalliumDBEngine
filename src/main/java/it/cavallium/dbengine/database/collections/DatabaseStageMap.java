package it.cavallium.dbengine.database.collections;

import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.UpdateMode;
import it.cavallium.dbengine.database.collections.Joiner.ValueGetter;
import it.cavallium.dbengine.database.collections.JoinerBlocking.ValueGetterBlocking;
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
public interface DatabaseStageMap<T, U, US extends DatabaseStage<U>> extends DatabaseStageEntry<Map<T, U>> {

	Mono<US> at(@Nullable CompositeSnapshot snapshot, T key);

	default Mono<U> getValue(@Nullable CompositeSnapshot snapshot, T key, boolean existsAlmostCertainly) {
		return this.at(snapshot, key).flatMap(v -> v.get(snapshot, existsAlmostCertainly).doFinally(s -> v.release()));
	}

	default Mono<U> getValue(@Nullable CompositeSnapshot snapshot, T key) {
		return getValue(snapshot, key, false);
	}

	default Mono<U> getValueOrDefault(@Nullable CompositeSnapshot snapshot, T key, Mono<U> defaultValue) {
		return getValue(snapshot, key).switchIfEmpty(defaultValue).single();
	}

	default Mono<Void> putValue(T key, U value) {
		return at(null, key).single().flatMap(v -> v.set(value).doFinally(s -> v.release()));
	}

	Mono<UpdateMode> getUpdateMode();

	default Mono<Boolean> updateValue(T key, boolean existsAlmostCertainly, Function<@Nullable U, @Nullable U> updater) {
		return this
				.at(null, key)
				.single()
				.flatMap(v -> v
						.update(updater, existsAlmostCertainly)
						.doFinally(s -> v.release())
				);
	}

	default Mono<Boolean> updateValue(T key, Function<@Nullable U, @Nullable U> updater) {
		return updateValue(key, false, updater);
	}

	default Mono<U> putValueAndGetPrevious(T key, U value) {
		return at(null, key).single().flatMap(v -> v.setAndGetPrevious(value).doFinally(s -> v.release()));
	}

	/**
	 *
	 * @param key
	 * @param value
	 * @return true if the key was associated with any value, false if the key didn't exist.
	 */
	default Mono<Boolean> putValueAndGetChanged(T key, U value) {
		return at(null, key).single().flatMap(v -> v.setAndGetChanged(value).doFinally(s -> v.release())).single();
	}

	default Mono<Void> remove(T key) {
		return removeAndGetStatus(key).then();
	}

	default Mono<U> removeAndGetPrevious(T key) {
		return at(null, key).flatMap(v -> v.clearAndGetPrevious().doFinally(s -> v.release()));
	}

	default Mono<Boolean> removeAndGetStatus(T key) {
		return removeAndGetPrevious(key).map(o -> true).defaultIfEmpty(false);
	}

	default Flux<Entry<T, U>> getMulti(@Nullable CompositeSnapshot snapshot, Flux<T> keys, boolean existsAlmostCertainly) {
		return keys.flatMapSequential(key -> this
				.getValue(snapshot, key, existsAlmostCertainly)
				.map(value -> Map.entry(key, value)));
	}

	default Flux<Entry<T, U>> getMulti(@Nullable CompositeSnapshot snapshot, Flux<T> keys) {
		return getMulti(snapshot, keys, false);
	}

	default Mono<Void> putMulti(Flux<Entry<T, U>> entries) {
		return entries.flatMap(entry -> this.putValue(entry.getKey(), entry.getValue())).then();
	}

	Flux<Entry<T, US>> getAllStages(@Nullable CompositeSnapshot snapshot);

	default Flux<Entry<T, U>> getAllValues(@Nullable CompositeSnapshot snapshot) {
		return this
				.getAllStages(snapshot)
				.flatMapSequential(entry -> entry
						.getValue()
						.get(snapshot, true)
						.map(value -> Map.entry(entry.getKey(), value))
						.doFinally(s -> entry.getValue().release())
				);
	}

	default Mono<Void> setAllValues(Flux<Entry<T, U>> entries) {
		return setAllValuesAndGetPrevious(entries).then();
	}

	Flux<Entry<T, U>> setAllValuesAndGetPrevious(Flux<Entry<T, U>> entries);

	default Mono<Void> clear() {
		return setAllValues(Flux.empty());
	}

	default Mono<Void> replaceAllValues(boolean canKeysChange, Function<Entry<T, U>, Mono<Entry<T, U>>> entriesReplacer) {
		if (canKeysChange) {
			return this.setAllValues(this.getAllValues(null).flatMap(entriesReplacer)).then();
		} else {
			return this
					.getAllValues(null)
					.flatMap(entriesReplacer)
					.flatMap(replacedEntry -> this
							.at(null, replacedEntry.getKey())
							.flatMap(v -> v.set(replacedEntry.getValue()).doFinally(s -> v.release())))
					.then();
		}
	}

	default Mono<Void> replaceAll(Function<Entry<T, US>, Mono<Void>> entriesReplacer) {
		return this
				.getAllStages(null)
				.flatMap(stage -> Mono
						.defer(() -> entriesReplacer.apply(stage))
						.doFinally(s -> stage.getValue().release())
				)
				.then();
	}

	@Override
	default Mono<Map<T, U>> setAndGetPrevious(Map<T, U> value) {
		return this
				.setAllValuesAndGetPrevious(Flux.fromIterable(value.entrySet()))
				.collectMap(Entry::getKey, Entry::getValue, HashMap::new);
	}

	@Override
	default Mono<Boolean> setAndGetChanged(Map<T, U> value) {
		return this
				.setAndGetPrevious(value)
				.map(oldValue -> !Objects.equals(oldValue, value))
				.switchIfEmpty(Mono.fromSupplier(() -> !value.isEmpty()));
	}

	@Override
	default Mono<Boolean> update(Function<@Nullable Map<T, U>, @Nullable Map<T, U>> updater, boolean existsAlmostCertainly) {
		return this
				.getUpdateMode()
				.single()
				.flatMap(updateMode -> {
					if (updateMode == UpdateMode.ALLOW_UNSAFE) {
						return this
								.getAllValues(null)
								.collectMap(Entry::getKey, Entry::getValue, HashMap::new)
								.single()
								.<Tuple2<Optional<Map<T, U>>, Boolean>>handle((v, sink) -> {
									if (v.isEmpty()) {
										v = null;
									}
									var result = updater.apply(v);
									if (result != null && result.isEmpty()) {
										result = null;
									}
									boolean changed = !Objects.equals(v, result);
									sink.next(Tuples.of(Optional.ofNullable(result), changed));
								})
								.flatMap(result -> Mono
										.justOrEmpty(result.getT1())
										.flatMap(values -> this.setAllValues(Flux.fromIterable(values.entrySet())))
										.thenReturn(result.getT2())
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
	default Mono<Map<T, U>> clearAndGetPrevious() {
		return this.setAndGetPrevious(Map.of());
	}

	@Override
	default Mono<Map<T, U>> get(@Nullable CompositeSnapshot snapshot, boolean existsAlmostCertainly) {
		return getAllValues(snapshot)
				.collectMap(Entry::getKey, Entry::getValue, HashMap::new);
	}

	@Override
	default Mono<Long> leavesCount(@Nullable CompositeSnapshot snapshot, boolean fast) {
		return getAllStages(snapshot)
				.flatMap(stage -> Mono
						.fromRunnable(() -> stage.getValue().release())
						.thenReturn(true)
				)
				.count();
	}

	/**
	 * Value getter doesn't lock data. Please make sure to lock before getting data.
	 */
	default ValueGetterBlocking<T, U> getDbValueGetter(@Nullable CompositeSnapshot snapshot) {
		return k -> getValue(snapshot, k).block();
	}

	/**
	 * Value getter doesn't lock data. Please make sure to lock before getting data.
	 */
	default ValueGetter<T, U> getAsyncDbValueGetter(@Nullable CompositeSnapshot snapshot) {
		return k -> getValue(snapshot, k);
	}
}
