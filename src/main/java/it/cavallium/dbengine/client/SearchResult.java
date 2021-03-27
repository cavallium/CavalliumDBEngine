package it.cavallium.dbengine.client;

import lombok.Value;
import reactor.core.publisher.Flux;

@Value
public class SearchResult<T, U> {

	Flux<SearchResultItem<T, U>> results;
	long totalHitsCount;

	public static <T, U> SearchResult<T, U> empty() {
		return new SearchResult<>(Flux.empty(), 0L);
	}
}
