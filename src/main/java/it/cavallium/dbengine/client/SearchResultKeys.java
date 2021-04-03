package it.cavallium.dbengine.client;

import it.cavallium.dbengine.database.collections.Joiner.ValueGetter;
import lombok.Value;
import reactor.core.publisher.Flux;

@SuppressWarnings("unused")
@Value
public class SearchResultKeys<T> {

	Flux<SearchResultKey<T>> results;
	long totalHitsCount;

	public static <T, U> SearchResultKeys<T> empty() {
		return new SearchResultKeys<>(Flux.empty(), 0L);
	}

	public <U> SearchResult<T, U> withValues(ValueGetter<T, U> valuesGetter) {
		return new SearchResult<>(
				results.flatMapSequential(item -> valuesGetter
						.get(item.getKey())
						.map(value -> new SearchResultItem<>(item.getKey(), value, item.getScore()))),
				totalHitsCount
		);
	}
}
