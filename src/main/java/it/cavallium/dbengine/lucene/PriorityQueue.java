package it.cavallium.dbengine.lucene;

import java.io.Closeable;
import java.util.Iterator;

public interface PriorityQueue<T> extends ResourceIterable<T>, Closeable {

	/**
	 * Adds an Object to a PriorityQueue in log(size) time. If one tries to add more objects than maxSize from initialize
	 * an {@link ArrayIndexOutOfBoundsException} is thrown.
	 */
	void add(T element);

	/**
	 * Returns the least element of the PriorityQueue in constant time.
	 */
	T top();

	/**
	 * Removes and returns the least element of the PriorityQueue in log(size) time.
	 */
	T pop();

	/**
	 * Should be called when the Object at top changes values. Still log(n) worst case, but it's at least twice as fast
	 * to
	 *
	 * <pre class="prettyprint">
	 * pq.top().change();
	 * pq.updateTop();
	 * </pre>
	 * <p>
	 * instead of
	 *
	 * <pre class="prettyprint">
	 * o = pq.pop();
	 * o.change();
	 * pq.push(o);
	 * </pre>
	 */
	void updateTop();

	/**
	 * Replace the top of the pq with {@code newTop} and run {@link #updateTop()}.
	 */
	void updateTop(T newTop);

	/**
	 * Returns the number of elements currently stored in the PriorityQueue.
	 */
	long size();

	/**
	 * Removes all entries from the PriorityQueue.
	 */
	void clear();

	/**
	 * Removes an existing element currently stored in the PriorityQueue. Cost is linear with the size of the queue. (A
	 * specialization of PriorityQueue which tracks element positions would provide a constant remove time but the
	 * trade-off would be extra cost to all additions/insertions)
	 */
	boolean remove(T element);
}
