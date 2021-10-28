package it.cavallium.dbengine.client;

import org.jetbrains.annotations.NotNull;
import reactor.core.publisher.Mono;

public record LazyHitEntry<T, U>(Mono<T> key, Mono<U> value, float score) {

	public Mono<HitEntry<T, U>> resolve() {
		return Mono.zip(key, value, (k, v) -> new HitEntry<>(k, v, score));
	}
}
