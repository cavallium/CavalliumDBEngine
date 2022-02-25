package it.cavallium.dbengine.lucene.searcher;

import java.time.Duration;
import java.util.function.Function;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class TimeoutUtil {

	private static final Duration INFINITE = Duration.ofDays(360);

	public static <T> Function<Mono<T>, Mono<T>> timeoutMono(Duration timeout) {
		return query -> {
			if (timeout.isZero() || timeout.isNegative() || timeout.compareTo(INFINITE) > 0) {
				return query;
			} else {
				return query.timeout(timeout);
			}
		};
	}

	public static <T> Function<Flux<T>, Flux<T>> timeoutFlux(Duration timeout) {
		return query -> {
			if (timeout.compareTo(INFINITE) > 0) {
				return query;
			} else {
				return query.timeout(timeout);
			}
		};
	}
}
