package it.cavallium.dbengine.database.collections;

import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.collections.Joiner.ValueGetter;
import it.cavallium.dbengine.database.collections.JoinerBlocking.ValueGetterBlocking;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Function;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@SuppressWarnings("unused")
public interface DatabaseStageMap<T, U, US extends DatabaseStage<U>> extends DatabaseStageEntry<Map<T, U>> {

	Mono<US> at(@Nullable CompositeSnapshot snapshot, T key);

	default Mono<U> getValue(@Nullable CompositeSnapshot snapshot, T key, boolean existsAlmostCertainly) {
		return this.at(snapshot, key).flatMap(v -> v.get(snapshot, existsAlmostCertainly));
	}

	default Mono<U> getValue(@Nullable CompositeSnapshot snapshot, T key) {
		return getValue(snapshot, key, false);
	}

	default Mono<U> getValueOrDefault(@Nullable CompositeSnapshot snapshot, T key, Mono<U> defaultValue) {
		return getValue(snapshot, key).switchIfEmpty(defaultValue).single();
	}

	default Mono<Void> putValue(T key, U value) {
		return at(null, key).single().flatMap(v -> v.set(value));
	}

	default Mono<Boolean> updateValue(T key, boolean existsAlmostCertainly, Function<Optional<U>, Optional<U>> updater) {
		return at(null, key).single().flatMap(v -> v.update(updater, existsAlmostCertainly));
	}

	default Mono<Boolean> updateValue(T key, Function<Optional<U>, Optional<U>> updater) {
		return updateValue(key, false, updater);
	}

	default Mono<U> putValueAndGetPrevious(T key, U value) {
		return at(null, key).single().flatMap(v -> v.setAndGetPrevious(value));
	}

	default Mono<Boolean> putValueAndGetStatus(T key, U value) {
		return at(null, key).single().flatMap(v -> v.setAndGetStatus(value));
	}

	default Mono<Void> remove(T key) {
		return removeAndGetStatus(key).then();
	}

	default Mono<U> removeAndGetPrevious(T key) {
		return at(null, key).flatMap(DatabaseStage::clearAndGetPrevious);
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
							.map(entry -> entry.set(replacedEntry.getValue())))
					.then();
		}
	}

	default Mono<Void> replaceAll(Function<Entry<T, US>, Mono<Void>> entriesReplacer) {
		return this
				.getAllStages(null)
				.flatMap(entriesReplacer)
				.then();
	}

	@Override
	default Mono<Map<T, U>> setAndGetPrevious(Map<T, U> value) {
		return this
				.setAllValuesAndGetPrevious(Flux.fromIterable(value.entrySet()))
				.collectMap(Entry::getKey, Entry::getValue, HashMap::new);
	}

	@Override
	default Mono<Boolean> update(Function<Optional<Map<T, U>>, Optional<Map<T, U>>> updater, boolean existsAlmostCertainly) {
		return this
				.getAllValues(null)
				.collectMap(Entry::getKey, Entry::getValue, HashMap::new)
				.single()
				.map(v -> v.isEmpty() ? Optional.<Map<T, U>>empty() : Optional.of(v))
				.map(updater)
				.filter(Optional::isPresent)
				.map(Optional::get)
				.flatMap(values -> this.setAllValues(Flux.fromIterable(values.entrySet())))
				//todo: can be optimized by calculating the correct return value
				.thenReturn(true);
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
		return getAllStages(snapshot).count();
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
