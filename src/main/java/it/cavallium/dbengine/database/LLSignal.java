package it.cavallium.dbengine.database;

public interface LLSignal {

	boolean isValue();

	boolean isTotalHitsCount();

	LLKeyScore getValue();

	long getTotalHitsCount();

	static LLSignal value(LLKeyScore value) {
		return value;
	}

	static LLTotalHitsCount totalHitsCount(long totalHitsCount) {
		return new LLTotalHitsCount(totalHitsCount);
	}
}
