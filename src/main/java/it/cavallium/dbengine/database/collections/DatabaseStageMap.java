package it.cavallium.dbengine.database.collections;

import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.collections.Joiner.ValueGetter;
import it.cavallium.dbengine.database.collections.JoinerBlocking.ValueGetterBlocking;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@SuppressWarnings("unused")
public interface DatabaseStageMap<T, U, US extends DatabaseStage<U>> extends DatabaseStageEntry<Map<T, U>> {

	Mono<US> at(@Nullable CompositeSnapshot snapshot, T key);

	default Mono<U> getValue(@Nullable CompositeSnapshot snapshot, T key) {
		return this.at(snapshot, key).flatMap(v -> v.get(snapshot));
	}

	default Mono<U> getValueOrDefault(@Nullable CompositeSnapshot snapshot, T key, Mono<U> defaultValue) {
		return getValue(snapshot, key).switchIfEmpty(defaultValue).single();
	}

	default Mono<Void> putValue(T key, U value) {
		return at(null, key).single().flatMap(v -> v.set(value));
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

	default Flux<Entry<T, U>> getMulti(@Nullable CompositeSnapshot snapshot, Flux<T> keys) {
		return keys.flatMap(key -> this.getValue(snapshot, key).map(value -> Map.entry(key, value)));
	}

	default Mono<Void> putMulti(Flux<Entry<T, U>> entries) {
		return entries.flatMap(entry -> this.putValue(entry.getKey(), entry.getValue())).then();
	}

	Flux<Entry<T, US>> getAllStages(@Nullable CompositeSnapshot snapshot);

	default Flux<Entry<T, U>> getAllValues(@Nullable CompositeSnapshot snapshot) {
		return this
				.getAllStages(null)
				.flatMap(entry -> entry.getValue().get(null).map(value -> Map.entry(entry.getKey(), value)));
	}

	default Mono<Void> setAllValues(Flux<Entry<T, U>> entries) {
		return setAllValuesAndGetPrevious(entries).then();
	}

	Flux<Entry<T, U>> setAllValuesAndGetPrevious(Flux<Entry<T, U>> entries);

	default Mono<Void> clear() {
		return setAllValues(Flux.empty());
	}

	default Mono<Void> replaceAllValues(boolean canKeysChange, Function<Entry<T, U>, Mono<Entry<T, U>>> entriesReplacer) {
		return Mono.defer(() -> {
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
		});
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
	default Mono<Map<T, U>> clearAndGetPrevious() {
		return this.setAndGetPrevious(Map.of());
	}

	@Override
	default Mono<Map<T, U>> get(@Nullable CompositeSnapshot snapshot) {
		return getAllValues(snapshot)
				.collectMap(Entry::getKey, Entry::getValue, HashMap::new);
	}

	@Override
	default Mono<Long> size(@Nullable CompositeSnapshot snapshot, boolean fast) {
		return getAllStages(snapshot).count();
	}

	/**
	 * Value getter doesn't lock data. Please make sure to lock before getting data.
	 */
	default ValueGetterBlocking<T, U> getDbValueGetter() {
		return k -> getValue(null, k).block();
	}

	/**
	 * Value getter doesn't lock data. Please make sure to lock before getting data.
	 */
	default ValueGetter<T, U> getAsyncDbValueGetter() {
		return k -> getValue(null, k);
	}
}
