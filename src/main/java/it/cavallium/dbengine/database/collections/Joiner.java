package it.cavallium.dbengine.database.collections;

import reactor.core.publisher.Mono;

public interface Joiner<KEY, DB_VALUE, JOINED_VALUE> {

	interface ValueGetter<KEY, VALUE> {

		/**
		 * Can return Mono error IOException
		 */
		Mono<VALUE> get(KEY key);
	}

	/**
	 * Warning! You must only join with immutable data to ensure data correctness.
	 * Good examples: message id, send date, ...
	 * Bad examples: message content, views, edited, ...
	 *
	 * Can return Mono error IOException
	 */
	Mono<JOINED_VALUE> join(ValueGetter<KEY, DB_VALUE> dbValueGetter, DB_VALUE value);

}
