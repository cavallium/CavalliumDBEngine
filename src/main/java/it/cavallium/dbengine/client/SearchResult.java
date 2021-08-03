package it.cavallium.dbengine.client;

import it.cavallium.dbengine.client.query.current.data.TotalHitsCount;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public record SearchResult<T, U>(Flux<SearchResultItem<T, U>> results, TotalHitsCount totalHitsCount, Mono<Void> release) {

	public static <T, U> SearchResult<T, U> empty() {
		return new SearchResult<>(Flux.empty(), TotalHitsCount.of(0, true), Mono.empty());
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
