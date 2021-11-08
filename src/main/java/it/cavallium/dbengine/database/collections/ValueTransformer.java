package it.cavallium.dbengine.database.collections;

import java.util.Map.Entry;
import java.util.Optional;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuple3;

public interface ValueTransformer<KEY, VALUE> {

	/**
	 * Can return Flux error IOException
	 */
	Flux<Entry<KEY, Optional<VALUE>>> transform(Flux<KEY> keys);
}
