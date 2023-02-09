package it.cavallium.dbengine.lucene;

import java.util.stream.Stream;

public class EmptyPriorityQueue<T> implements PriorityQueue<T> {

	@Override
	public void add(T element) {
		throw new UnsupportedOperationException();
	}

	@Override
	public T top() {
		return null;
	}

	@Override
	public T pop() {
		return null;
	}

	@Override
	public void replaceTop(T oldTop, T newTop) {
		assert oldTop == null;
		assert newTop == null;
	}

	@Override
	public long size() {
		return 0;
	}

	@Override
	public void clear() {

	}

	@Override
	public boolean remove(T element) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Stream<T> iterate() {
		return Stream.empty();
	}

	@Override
	public void close() {

	}
}
