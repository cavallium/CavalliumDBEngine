package it.cavallium.dbengine.lucene;

import it.cavallium.dbengine.database.DiscardingCloseable;
import java.util.stream.Stream;

public interface ResourceIterable<T> extends DiscardingCloseable {

	/**
	 * Iterate this PriorityQueue
	 */
	Stream<T> iterate();

	/**
	 * Iterate this PriorityQueue
	 */
	default Stream<T> iterate(long skips) {
		if (skips == 0) {
			return iterate();
		} else {
			return iterate().skip(skips);
		}
	}
}
