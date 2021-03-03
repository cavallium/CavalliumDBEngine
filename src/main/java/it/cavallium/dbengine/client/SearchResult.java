package it.cavallium.dbengine.client;

import lombok.EqualsAndHashCode;
import lombok.ToString;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

@EqualsAndHashCode
@ToString
public class SearchResult<T, U> {

	private final Flux<LuceneSignal<SearchResultItem<T, U>>> results;

	public SearchResult(Flux<LuceneSignal<SearchResultItem<T, U>>> results) {
		this.results = results;
	}

	public static <T, U> SearchResult<T, U> empty() {
		return new SearchResult<>(Flux.just(LuceneSignal.totalHitsCount(0L)));
	}

	public Flux<LuceneSignal<SearchResultItem<T, U>>> results() {
		return this.results;
	}

	public Tuple2<Flux<SearchResultItem<T, U>>, Mono<Long>> splitShared() {
		Flux<LuceneSignal<SearchResultItem<T, U>>> shared = results.publish().refCount(2);
		return Tuples.of(
				shared.filter(LuceneSignal::isValue).map(LuceneSignal::getValue).share(),
				Mono.from(shared.filter(LuceneSignal::isTotalHitsCount).map(LuceneSignal::getTotalHitsCount)).cache()
		);
	}
}
