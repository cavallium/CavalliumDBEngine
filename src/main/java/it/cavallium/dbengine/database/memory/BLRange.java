package it.cavallium.dbengine.database.memory;

import it.cavallium.dbengine.buffers.Buf;

public class BLRange {

	private final Buf min;
	private final Buf max;
	private final Buf single;

	public BLRange(Buf min, Buf max, Buf single) {
		if (single != null && (min != null || max != null)) {
			throw new IllegalArgumentException();
		}
		this.min = min;
		this.max = max;
		this.single = single;
	}

	public Buf getMin() {
		return min;
	}

	public Buf getMax() {
		return max;
	}

	public Buf getSingle() {
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
