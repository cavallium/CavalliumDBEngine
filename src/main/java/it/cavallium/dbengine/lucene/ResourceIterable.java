package it.cavallium.dbengine.lucene;

import java.util.Iterator;
import org.jetbrains.annotations.NotNull;
import reactor.core.publisher.Flux;

public interface ResourceIterable<T> {

	/**
	 * Iterate this PriorityQueue
	 */
	Flux<T> iterate();

	/**
	 * Iterate this PriorityQueue
	 */
	default Flux<T> iterate(long skips) {
		if (skips == 0) {
			return iterate();
		} else {
			return iterate().skip(skips);
		}
	}
}
