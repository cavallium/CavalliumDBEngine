package it.cavallium.dbengine.database.collections;

import reactor.core.publisher.Mono;

public interface ValueGetter<KEY, VALUE> {

	/**
	 * Can return Mono error IOException
	 */
	Mono<VALUE> get(KEY key);
}
