package it.cavallium.dbengine.database;

import java.util.StringJoiner;

public class LLSnapshot {
	private final long sequenceNumber;

	public LLSnapshot(long sequenceNumber) {
		this.sequenceNumber = sequenceNumber;
	}

	public long getSequenceNumber() {
		return sequenceNumber;
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", LLSnapshot.class.getSimpleName() + "[", "]")
				.add("sequenceNumber=" + sequenceNumber)
				.toString();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		LLSnapshot that = (LLSnapshot) o;

		return sequenceNumber == that.sequenceNumber;
	}

	@Override
	public int hashCode() {
		return (int) (sequenceNumber ^ (sequenceNumber >>> 32));
	}
}
