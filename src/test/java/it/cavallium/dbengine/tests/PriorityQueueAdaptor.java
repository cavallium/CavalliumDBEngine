package it.cavallium.dbengine.tests;

import it.cavallium.dbengine.database.DiscardingCloseable;
import it.cavallium.dbengine.lucene.PriorityQueue;
import it.cavallium.dbengine.utils.SimpleResource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;
import org.apache.lucene.search.HitQueue;

public class PriorityQueueAdaptor<T> extends SimpleResource implements PriorityQueue<T>, DiscardingCloseable {

	private final org.apache.lucene.util.PriorityQueue<T> hitQueue;

	public PriorityQueueAdaptor(org.apache.lucene.util.PriorityQueue<T> hitQueue) {
		this.hitQueue = hitQueue;
	}

	@Override
	public void add(T element) {
		hitQueue.add(element);
		hitQueue.updateTop();
	}

	@Override
	public T top() {
		hitQueue.updateTop();
		return hitQueue.top();
	}

	@Override
	public T pop() {
		var popped = hitQueue.pop();
		hitQueue.updateTop();
		return popped;
	}

	@Override
	public void replaceTop(T oldTop, T newTop) {
		assert Objects.equals(oldTop, hitQueue.top());
		hitQueue.updateTop(newTop);
	}

	@Override
	public long size() {
		return hitQueue.size();
	}

	@Override
	public void clear() {
		hitQueue.clear();
	}

	@Override
	public boolean remove(T element) {
		var removed = hitQueue.remove(element);
		hitQueue.updateTop();
		return removed;
	}

	@Override
	public Stream<T> iterate() {
		List<T> items = new ArrayList<>(hitQueue.size());
		T item;
		while ((item = hitQueue.pop()) != null) {
			items.add(item);
		}
		for (T t : items) {
			hitQueue.insertWithOverflow(t);
		}
		return items.stream();
	}

	@Override
	protected void onClose() {
		hitQueue.clear();
		hitQueue.updateTop();
	}
}
