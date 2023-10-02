package it.cavallium.dbengine.database;

import it.cavallium.buffer.Buf;
import java.util.Objects;
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
		assert min == null || max == null || min.compareTo(max) <= 0
				: "Minimum buffer is bigger than maximum buffer: " + min + " > " + max;
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

	public static boolean isInside(LLRange rangeSub, LLRange rangeParent) {
		if (rangeParent.isAll()) {
			return true;
		} else if (rangeParent.isSingle()) {
			return Objects.equals(rangeSub, rangeParent);
		} else {
			return ((!rangeParent.hasMin() || (rangeSub.hasMin() && rangeParent.getMin().compareTo(rangeSub.getMin()) <= 0)))
					&& ((!rangeParent.hasMax() || (rangeSub.hasMax() && rangeParent.getMax().compareTo(rangeSub.getMax()) >= 0)));
		}
	}

	@Nullable
	public static LLRange intersect(LLRange rangeA, LLRange rangeB) {
		boolean aEndInclusive = rangeA.isSingle();
		boolean bEndInclusive = rangeB.isSingle();
		Buf min = rangeA.isAll()
				? rangeB.getMin()
				: (rangeB.isAll()
						? rangeA.getMin()
						: (rangeA.getMin().compareTo(rangeB.getMin()) <= 0 ? rangeB.getMin() : rangeA.getMin()));
		int aComparedToB;
		Buf max;
		boolean maxInclusive;
		if (rangeA.isAll()) {
			max = rangeB.getMax();
			maxInclusive = bEndInclusive;
		} else if (rangeB.isAll()) {
			max = rangeA.getMax();
			maxInclusive = aEndInclusive;
		} else if ((aComparedToB = rangeA.getMax().compareTo(rangeB.getMax())) >= 0) {
			max = rangeB.getMax();
			if (aComparedToB == 0) {
				maxInclusive = bEndInclusive && aEndInclusive;
			} else {
				maxInclusive = bEndInclusive;
			}
		} else {
			max = rangeA.getMax();
			maxInclusive = aEndInclusive;
		}
		if (min != null && max != null && min.compareTo(max) >= (maxInclusive ? 1 : 0)) {
			return null;
		} else {
			if (min != null && min.equals(max)) {
				return LLRange.single(min);
			} else {
				return LLRange.of(min, max);
			}
		}
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

	@SuppressWarnings("UnnecessaryUnicodeEscape")
	@Override
	public String toString() {
		if (single != null) {
			return "[" + single + "]";
		} else if (min != null && max != null) {
			return "[" + LLUtils.toString(min) + "," + LLUtils.toString(max) + ")";
		} else if (min != null) {
			return "[" + min + ",\u221E)";
		} else if (max != null) {
			return "[\u2205," + max + ")";
		} else {
			return "[\u221E)";
		}
	}

	public LLRange copy() {
		// todo: use a read-only copy
		return new LLRange(min, max, single);
	}
}
