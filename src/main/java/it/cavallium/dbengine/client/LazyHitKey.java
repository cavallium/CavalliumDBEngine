package it.cavallium.dbengine.client;

import java.util.function.Function;
import reactor.core.publisher.Mono;

public record LazyHitKey<T>(Mono<T> key, float score) {

	public <U> LazyHitEntry<T, U> withValue(Function<T, Mono<U>> valueGetter) {
		return new LazyHitEntry<>(key, key.flatMap(valueGetter), score);
	}

	public Mono<HitKey<T>> resolve() {
		return key.map(k -> new HitKey<>(k, score));
	}

	public <U> Mono<HitEntry<T, U>> resolveWithValue(Function<T, Mono<U>> valueGetter) {
		return resolve().flatMap(key -> key.withValue(valueGetter));
	}
}
