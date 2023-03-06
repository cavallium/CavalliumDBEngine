package it.cavallium.dbengine.database;

import it.cavallium.buffer.Buf;
import java.util.StringJoiner;
import org.jetbrains.annotations.Nullable;

/**
 * Range of data, from min (inclusive), to max (exclusive)
 */
public class LLRange {

	private static final LLRange RANGE_ALL = new LLRange( null, null, (Buf) null);
	@Nullable
	private final Buf min;
	@Nullable
	private final Buf max;
	@Nullable
	private final Buf single;

	private LLRange(@Nullable Buf min, @Nullable Buf max, @Nullable Buf single) {
		assert single == null || (min == null && max == null);
		this.min = min;
		this.max = max;
		this.single = single;
	}

	public static LLRange all() {
		return RANGE_ALL;
	}

	public static LLRange from(Buf min) {
		return new LLRange(min, null, null);
	}

	public static LLRange to(Buf max) {
		return new LLRange(null, max, null);
	}

	public static LLRange single(Buf single) {
		return new LLRange(null, null, single);
	}

	public static LLRange of(Buf min, Buf max) {
		return new LLRange(min, max, null);
	}

	public boolean isAll() {
		return min == null && max == null && single == null;
	}

	public boolean isSingle() {
		return single != null;
	}

	public boolean hasMin() {
		return min != null || single != null;
	}

	public Buf getMin() {
		// todo: use a read-only copy
		if (min != null) {
			return min;
		} else {
			return single;
		}
	}

	public boolean hasMax() {
		return max != null || single != null;
	}

	public Buf getMax() {
		// todo: use a read-only copy
		if (max != null) {
			return max;
		} else {
			return single;
		}
	}

	public Buf getSingle() {
		assert isSingle();
		// todo: use a read-only copy
		return single;
	}

	public Buf getSingleUnsafe() {
		assert isSingle();
		return single;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		LLRange llRange = (LLRange) o;
		return LLUtils.equals(min, llRange.min) && LLUtils.equals(max, llRange.max);
	}

	@Override
	public int hashCode() {
		int result = LLUtils.hashCode(min);
		result = 31 * result + LLUtils.hashCode(max);
		return result;
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", LLRange.class.getSimpleName() + "[", "]")
				.add("min=" + LLUtils.toString(min))
				.add("max=" + LLUtils.toString(max))
				.toString();
	}

	public LLRange copy() {
		// todo: use a read-only copy
		return new LLRange(min, max, single);
	}
}
