package it.cavallium.dbengine.client;

public class LuceneSignalTotalHitsCount<T> implements LuceneSignal<T> {

	private final long totalHitsCount;

	public LuceneSignalTotalHitsCount(long totalHitsCount) {
		this.totalHitsCount = totalHitsCount;
	}

	@Override
	public boolean isValue() {
		return false;
	}

	@Override
	public boolean isTotalHitsCount() {
		return true;
	}

	@Override
	public T getValue() {
		throw new UnsupportedOperationException("This object is not value");
	}

	@Override
	public long getTotalHitsCount() {
		return totalHitsCount;
	}

	@Override
	public <U> LuceneSignalTotalHitsCount<U> mapTotalHitsCount() {
		//noinspection unchecked
		return (LuceneSignalTotalHitsCount<U>) this;
	}
}
