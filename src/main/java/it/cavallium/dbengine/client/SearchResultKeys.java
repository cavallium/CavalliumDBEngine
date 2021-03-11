package it.cavallium.dbengine.client;

import it.cavallium.dbengine.database.collections.Joiner.ValueGetter;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

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

	public Flux<SearchResultKey<T>> onlyKeys() {
		return this.results.filter(LuceneSignal::isValue).map(LuceneSignal::getValue);
	}

	/**
	 * You must subscribe to both publishers
	 */
	public Tuple2<Flux<SearchResultKey<T>>, Mono<Long>> splitShared() {
		Flux<LuceneSignal<SearchResultKey<T>>> shared = results.publish().refCount(2);
		return Tuples.of(
				shared.filter(LuceneSignal::isValue).map(LuceneSignal::getValue).share(),
				Mono.from(shared.filter(LuceneSignal::isTotalHitsCount).map(LuceneSignal::getTotalHitsCount)).cache()
		);
	}
}
