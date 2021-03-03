package it.cavallium.dbengine.client;

import it.cavallium.dbengine.database.collections.Joiner.ValueGetter;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@EqualsAndHashCode
@ToString
public class SearchResultKeys<T> {

	private final Flux<LuceneSignal<SearchResultKey<T>>> results;

	public SearchResultKeys(Flux<LuceneSignal<SearchResultKey<T>>> results) {
		this.results = results;
	}

	public static <T, U> SearchResultKeys<T> empty() {
		return new SearchResultKeys<>(Flux.just(LuceneSignal.totalHitsCount(0L)));
	}

	public Flux<LuceneSignal<SearchResultKey<T>>> results() {
		return this.results;
	}

	public <U> SearchResult<T, U> withValues(ValueGetter<T, U> valuesGetter) {
		return new SearchResult<>(
				results.flatMapSequential(item -> {
					if (item.isValue()) {
						return valuesGetter
								.get(item.getValue().getKey())
								.map(value -> LuceneSignal.value(new SearchResultItem<>(item.getValue().getKey(), value, item.getValue().getScore())));
					} else {
						return Mono.just(item.mapTotalHitsCount());
					}
				})
		);
	}
}
