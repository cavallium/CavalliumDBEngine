package it.cavallium.dbengine.client;

import it.cavallium.dbengine.client.query.current.data.TotalHitsCount;
import it.cavallium.dbengine.database.collections.ValueGetter;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@SuppressWarnings("unused")
public record SearchResultKeys<T>(Flux<SearchResultKey<T>> results, TotalHitsCount totalHitsCount, Mono<Void> release) {

	public static <T, U> SearchResultKeys<T> empty() {
		return new SearchResultKeys<>(Flux.empty(), TotalHitsCount.of(0, true), Mono.empty());
	}

	public <U> SearchResult<T, U> withValues(ValueGetter<T, U> valuesGetter) {
		return new SearchResult<>(results.map(item -> new SearchResultItem<>(item.key(),
				item.key().flatMap(valuesGetter::get),
				item.score()
		)), totalHitsCount, release);
	}

	public Flux<SearchResultKey<T>> resultsThenRelease() {
		return Flux
				.usingWhen(
						Mono.just(true),
						_unused -> results,
						_unused -> release
				);
	}
}
