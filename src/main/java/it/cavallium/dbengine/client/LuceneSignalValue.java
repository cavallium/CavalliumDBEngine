package it.cavallium.dbengine.client;

public class LuceneSignalValue<T> implements LuceneSignal<T> {

	private final T value;

	public LuceneSignalValue(T value) {
		this.value = value;
	}

	public boolean isValue() {
		return true;
	}

	@Override
	public boolean isTotalHitsCount() {
		return false;
	}

	@Override
	public T getValue() {
		return value;
	}

	@Override
	public long getTotalHitsCount() {
		throw new UnsupportedOperationException("This object is not TotalHitsCount");
	}

	@Override
	public <U> LuceneSignalTotalHitsCount<U> mapTotalHitsCount() {
		throw new UnsupportedOperationException("This object is not TotalHitsCount");
	}
}
