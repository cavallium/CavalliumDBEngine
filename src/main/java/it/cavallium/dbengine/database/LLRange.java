package it.cavallium.dbengine.database;

import java.util.Arrays;
import java.util.StringJoiner;

public class LLRange {

	private static final LLRange RANGE_ALL = new LLRange(null, null);
	private final byte[] min;
	private final byte[] max;

	private LLRange(byte[] min, byte[] max) {
		this.min = min;
		this.max = max;
	}

	public static LLRange all() {
		return RANGE_ALL;
	}

	public static LLRange from(byte[] min) {
		return new LLRange(min, null);
	}

	public static LLRange to(byte[] max) {
		return new LLRange(null, max);
	}

	public static LLRange single(byte[] single) {
		return new LLRange(single, single);
	}

	public static LLRange of(byte[] min, byte[] max) {
		return new LLRange(min, max);
	}

	public boolean isAll() {
		return min == null && max == null;
	}

	public boolean isSingle() {
		if (min == null || max == null) return false;
		return Arrays.equals(min, max);
	}

	public boolean hasMin() {
		return min != null;
	}

	public byte[] getMin() {
		assert min != null;
		return min;
	}

	public boolean hasMax() {
		return max != null;
	}

	public byte[] getMax() {
		assert max != null;
		return max;
	}

	public byte[] getSingle() {
		assert isSingle();
		return min;
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
		return Arrays.equals(min, llRange.min) && Arrays.equals(max, llRange.max);
	}

	@Override
	public int hashCode() {
		int result = Arrays.hashCode(min);
		result = 31 * result + Arrays.hashCode(max);
		return result;
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", LLRange.class.getSimpleName() + "[", "]")
				.add("min=" + Arrays.toString(min))
				.add("max=" + Arrays.toString(max))
				.toString();
	}
}
