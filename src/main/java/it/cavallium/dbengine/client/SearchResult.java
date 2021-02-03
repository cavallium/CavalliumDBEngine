package it.cavallium.dbengine.client;

import java.util.Objects;
import java.util.StringJoiner;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class SearchResult<T, U> {

	private final Mono<Long> totalHitsCount;
	private final Flux<SearchResultItem<T, U>> results;

	public SearchResult(Mono<Long> totalHitsCount, Flux<SearchResultItem<T, U>> results) {
		this.totalHitsCount = totalHitsCount;
		this.results = results;
	}

	public static <T, U> SearchResult<T, U> empty() {
		return new SearchResult<>(Mono.just(0L), Flux.empty());
	}

	public Mono<Long> totalHitsCount() {
		return this.totalHitsCount;
	}

	public Flux<SearchResultItem<T, U>> results() {
		return this.results;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		SearchResult<?, ?> that = (SearchResult<?, ?>) o;
		return Objects.equals(totalHitsCount, that.totalHitsCount) && Objects.equals(results, that.results);
	}

	@Override
	public int hashCode() {
		return Objects.hash(totalHitsCount, results);
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", SearchResult.class.getSimpleName() + "[", "]")
				.add("totalHitsCount=" + totalHitsCount)
				.add("results=" + results)
				.toString();
	}
}
