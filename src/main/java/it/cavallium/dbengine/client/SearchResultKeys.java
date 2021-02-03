package it.cavallium.dbengine.client;

import it.cavallium.dbengine.database.collections.Joiner.ValueGetter;
import java.util.Objects;
import java.util.StringJoiner;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class SearchResultKeys<T> {

	private final Mono<Long> totalHitsCount;
	private final Flux<SearchResultKey<T>> results;

	public SearchResultKeys(Mono<Long> totalHitsCount, Flux<SearchResultKey<T>> results) {
		this.totalHitsCount = totalHitsCount;
		this.results = results;
	}

	public static <T, U> SearchResultKeys<T> empty() {
		return new SearchResultKeys<>(Mono.just(0L), Flux.empty());
	}

	public Mono<Long> totalHitsCount() {
		return this.totalHitsCount;
	}

	public Flux<SearchResultKey<T>> results() {
		return this.results;
	}

	public <U> SearchResult<T, U> withValues(ValueGetter<T, U> valuesGetter) {
		return new SearchResult<>(totalHitsCount,
				results.flatMap(item -> valuesGetter
						.get(item.getKey())
						.map(value -> new SearchResultItem<>(item.getKey(), value, item.getScore())))
		);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		SearchResultKeys<?> that = (SearchResultKeys<?>) o;
		return Objects.equals(totalHitsCount, that.totalHitsCount) && Objects.equals(results, that.results);
	}

	@Override
	public int hashCode() {
		return Objects.hash(totalHitsCount, results);
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", SearchResultKeys.class.getSimpleName() + "[", "]")
				.add("totalHitsCount=" + totalHitsCount)
				.add("results=" + results)
				.toString();
	}
}
