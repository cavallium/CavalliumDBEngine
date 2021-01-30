package it.cavallium.dbengine.database.collections;

import reactor.core.publisher.Mono;

public interface Joiner<KEY, DBVALUE, JOINEDVALUE> {

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
	Mono<JOINEDVALUE> join(ValueGetter<KEY, DBVALUE> dbValueGetter, DBVALUE value);

	static <KEY, DBVALUE> Joiner<KEY, DBVALUE, DBVALUE> direct() {
		return (dbValueGetter, value) -> Mono.just(value);
	};
}
