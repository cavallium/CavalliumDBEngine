package it.cavallium.dbengine.client;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public record HitEntry<T, U>(T key, @Nullable U value, float score)
		implements Comparable<HitEntry<T, U>> {

	@Override
	public int compareTo(@NotNull HitEntry<T, U> o) {
		return Float.compare(o.score, this.score);
	}
}
