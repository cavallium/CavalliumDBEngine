package it.cavallium.dbengine.client;

import it.cavallium.dbengine.database.collections.DatabaseEmpty.Nothing;
import java.util.Comparator;
import java.util.function.Function;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Mono;

public record HitKey<T>(T key, float score) implements Comparable<HitKey<T>> {

	public <U> Mono<HitEntry<T, U>> withValue(Function<T, Mono<U>> valueGetter) {
		return valueGetter.apply(key).map(value -> new HitEntry<>(key, value, score));
	}

	public <U> HitEntry<T, U> withNullValue() {
		return new HitEntry<>(key, null, score);
	}

	public HitEntry<T, Nothing> withNothingValue() {
		return new HitEntry<>(key, Nothing.INSTANCE, score);
	}

	@Override
	public int compareTo(@NotNull HitKey<T> o) {
		return Float.compare(o.score, this.score);
	}
}
