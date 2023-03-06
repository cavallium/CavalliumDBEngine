package it.cavallium.dbengine.database;

import static it.cavallium.dbengine.database.LLUtils.unmodifiableBytes;

import it.cavallium.buffer.Buf;
import java.util.StringJoiner;
import org.jetbrains.annotations.Nullable;

public class LLDelta {

	@Nullable
	private final Buf previous;
	@Nullable
	private final Buf current;

	private LLDelta(@Nullable Buf previous, @Nullable Buf current) {
		super();
		this.previous = unmodifiableBytes(previous);
		this.current = unmodifiableBytes(current);
	}

	public static LLDelta of(Buf previous, Buf current) {
		assert (previous == null && current == null) || (previous != current);
		return new LLDelta(previous, current);
	}

	public Buf previous() {
		return previous;
	}

	public Buf current() {
		return current;
	}

	public boolean isModified() {
		return !LLUtils.equals(previous, current);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		LLDelta LLDelta = (LLDelta) o;
		return LLUtils.equals(previous, LLDelta.previous) && LLUtils.equals(current, LLDelta.current);
	}

	@Override
	public int hashCode() {
		int result = LLUtils.hashCode(previous);
		result = 31 * result + LLUtils.hashCode(current);
		return result;
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", LLDelta.class.getSimpleName() + "[", "]")
				.add("min=" + LLUtils.toStringSafe(previous))
				.add("max=" + LLUtils.toStringSafe(current))
				.toString();
	}

}
