package it.cavallium.dbengine.client;

import java.util.Comparator;
import java.util.function.Function;
import org.jetbrains.annotations.NotNull;
import reactor.core.publisher.Mono;

public record HitKey<T>(T key, float score) implements Comparable<HitKey<T>> {

	public <U> Mono<HitEntry<T, U>> withValue(Function<T, Mono<U>> valueGetter) {
		return valueGetter.apply(key).map(value -> new HitEntry<>(key, value, score));
	}

	@Override
	public int compareTo(@NotNull HitKey<T> o) {
		return Float.compare(o.score, this.score);
	}
}
