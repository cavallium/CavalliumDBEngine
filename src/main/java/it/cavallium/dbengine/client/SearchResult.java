package it.cavallium.dbengine.client;

import reactor.core.publisher.Flux;

public record SearchResult<T, U>(Flux<SearchResultItem<T, U>> results, long totalHitsCount) {

	public static <T, U> SearchResult<T, U> empty() {
		return new SearchResult<>(Flux.empty(), 0L);
	}
}
