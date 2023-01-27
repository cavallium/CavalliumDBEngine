package it.cavallium.dbengine.utils;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class InternalMonoUtils {


	@SuppressWarnings("unchecked")
	public static <ANY> Mono<ANY> toAny(Mono<Void> request) {
		return (Mono<ANY>) request;
	}

	@SuppressWarnings("unchecked")
	public static <ANY> Mono<ANY> toAny(Flux<Void> request) {
		return (Mono<ANY>) Mono.ignoreElements(request);
	}

	@SuppressWarnings("unchecked")
	public static <ANY> Mono<ANY> toAny(Publisher<Void> request) {
		if (request instanceof Mono<Void> mono) {
			return (Mono<ANY>) mono;
		} else {
			return (Mono<ANY>) Mono.ignoreElements(request);
		}
	}

	@SuppressWarnings("unchecked")
	public static <ANY_IN, ANY_OUT> Mono<ANY_OUT> ignoreElements(Publisher<ANY_IN> flux) {
		return (Mono<ANY_OUT>) Mono.ignoreElements(flux);
	}
}
