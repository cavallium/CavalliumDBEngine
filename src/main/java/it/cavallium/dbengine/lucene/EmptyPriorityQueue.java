package it.cavallium.dbengine.lucene;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.jetbrains.annotations.NotNull;
import reactor.core.publisher.Flux;

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
	public void updateTop() {

	}

	@Override
	public void updateTop(T newTop) {
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
	public Flux<T> iterate() {
		return Flux.empty();
	}

	@Override
	public void close() throws IOException {

	}
}
