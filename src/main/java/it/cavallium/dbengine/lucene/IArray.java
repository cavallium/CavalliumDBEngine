package it.cavallium.dbengine.lucene;

import java.util.Objects;
import org.jetbrains.annotations.Nullable;

public interface IArray<T> {

	@Nullable T get(long index);

	void set(long index, @Nullable T value);

	void reset(long index);

	long size();

	default T getOrDefault(int slot, T defaultValue) {
		return Objects.requireNonNullElse(get(slot), defaultValue);
	}

}
