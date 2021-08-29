package it.cavallium.dbengine.database;

import java.util.Objects;
import org.jetbrains.annotations.Nullable;

public class Delta<T> {

	private final @Nullable T previous;
	private final @Nullable T current;

	public Delta(@Nullable T previous, @Nullable T current) {
		this.previous = previous;
		this.current = current;
	}

	public boolean isModified() {
		return !Objects.equals(previous, current);
	}

	public @Nullable T previous() {
		return previous;
	}

	public @Nullable T current() {
		return current;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this)
			return true;
		if (obj == null || obj.getClass() != this.getClass())
			return false;
		var that = (Delta) obj;
		return Objects.equals(this.previous, that.previous) && Objects.equals(this.current, that.current);
	}

	@Override
	public int hashCode() {
		return Objects.hash(previous, current);
	}

	@Override
	public String toString() {
		return "Delta[" + "previous=" + previous + ", " + "current=" + current + ']';
	}

}
