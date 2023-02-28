package it.cavallium.dbengine.client;

import java.util.Map;
import java.util.Map.Entry;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.Unmodifiable;

public record HitEntry<T, U>(T key, @Nullable U value, float score)
		implements Comparable<HitEntry<T, U>> {

	@Override
	public int compareTo(@NotNull HitEntry<T, U> o) {
		return Float.compare(o.score, this.score);
	}

	@Contract(pure = true)
	public @Nullable @Unmodifiable Entry<T, U> toEntry() {
		if (value != null) {
			return Map.entry(key, value);
		} else {
			return null;
		}
	}
}
