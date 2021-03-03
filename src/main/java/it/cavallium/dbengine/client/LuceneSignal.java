package it.cavallium.dbengine.client;

public interface LuceneSignal<T> {

	boolean isValue();

	boolean isTotalHitsCount();

	T getValue();

	long getTotalHitsCount();

	static <T> LuceneSignalValue<T> value(T value) {
		return new LuceneSignalValue<>(value);
	}

	static <T> LuceneSignalTotalHitsCount<T> totalHitsCount(long totalHitsCount) {
		return new LuceneSignalTotalHitsCount<>(totalHitsCount);
	}

	<U> LuceneSignalTotalHitsCount<U> mapTotalHitsCount();
}
