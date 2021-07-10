package it.cavallium.dbengine.client;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public record SearchResult<T, U>(Flux<SearchResultItem<T, U>> results, long totalHitsCount, Mono<Void> release) {

	public static <T, U> SearchResult<T, U> empty() {
		return new SearchResult<>(Flux.empty(), 0L, Mono.empty());
	}

	public Flux<SearchResultItem<T, U>> resultsThenRelease() {
		return Flux
				.usingWhen(
						Mono.just(true),
						_unused -> results,
						_unused -> release
				);
	}
}
