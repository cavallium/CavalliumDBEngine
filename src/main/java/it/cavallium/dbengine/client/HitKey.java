package it.cavallium.dbengine.client;

import it.cavallium.dbengine.database.collections.DatabaseEmpty.Nothing;
import java.util.function.Function;
import org.jetbrains.annotations.NotNull;

public record HitKey<T>(T key, float score) implements Comparable<HitKey<T>> {

	public <U> HitEntry<T, U> withValue(Function<T, U> valueGetter) {
		return new HitEntry<>(key, valueGetter.apply(key), score);
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
