package it.cavallium.dbengine.database.memory;

import it.unimi.dsi.fastutil.bytes.ByteList;

public class BLRange {

	private final ByteList min;
	private final ByteList max;
	private final ByteList single;

	public BLRange(ByteList min, ByteList max, ByteList single) {
		if (single != null && (min != null || max != null)) {
			throw new IllegalArgumentException();
		}
		this.min = min;
		this.max = max;
		this.single = single;
	}

	public ByteList getMin() {
		return min;
	}

	public ByteList getMax() {
		return max;
	}

	public ByteList getSingle() {
		return single;
	}

	public boolean isSingle() {
		return single != null;
	}

	public boolean isAll() {
		return single == null && min == null && max == null;
	}

	public boolean hasMin() {
		return min != null;
	}

	public boolean hasMax() {
		return max != null;
	}
}
