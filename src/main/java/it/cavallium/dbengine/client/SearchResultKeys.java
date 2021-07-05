package it.cavallium.dbengine.client;

import it.cavallium.dbengine.database.collections.Joiner.ValueGetter;
import reactor.core.publisher.Flux;

@SuppressWarnings("unused")
public record SearchResultKeys<T>(Flux<SearchResultKey<T>> results, long totalHitsCount) {

	public static <T, U> SearchResultKeys<T> empty() {
		return new SearchResultKeys<>(Flux.empty(), 0L);
	}

	public <U> SearchResult<T, U> withValues(ValueGetter<T, U> valuesGetter) {
		return new SearchResult<>(results.map(item -> new SearchResultItem<>(item.key(),
				item.key().flatMap(valuesGetter::get),
				item.score()
		)), totalHitsCount);
	}
}
