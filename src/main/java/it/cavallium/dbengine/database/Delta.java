package it.cavallium.dbengine.database;

import java.util.Objects;
import org.jetbrains.annotations.Nullable;

public record Delta<T>(@Nullable T previous, @Nullable T current) {

	public boolean isModified() {
		return !Objects.equals(previous, current);
	}
}
