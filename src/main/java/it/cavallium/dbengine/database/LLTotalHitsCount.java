package it.cavallium.dbengine.database;

import java.util.Objects;
import java.util.StringJoiner;

public class LLTotalHitsCount implements LLSignal {

	private final long value;

	public LLTotalHitsCount(long value) {
		this.value = value;
	}

	@Override
	public boolean isValue() {
		return false;
	}

	@Override
	public boolean isTotalHitsCount() {
		return true;
	}

	public LLKeyScore getValue() {
		throw new UnsupportedOperationException();
	}

	@Override
	public long getTotalHitsCount() {
		return value;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		LLTotalHitsCount that = (LLTotalHitsCount) o;
		return value == that.value;
	}

	@Override
	public int hashCode() {
		return Objects.hash(value);
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", LLTotalHitsCount.class.getSimpleName() + "[", "]").add("count=" + value).toString();
	}
}
