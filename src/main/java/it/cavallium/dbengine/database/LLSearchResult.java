package it.cavallium.dbengine.database;

import java.util.Objects;
import java.util.function.BiFunction;
import org.jetbrains.annotations.NotNull;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class LLSearchResult {

	private final Mono<Long> totalHitsCount;
	private final Flux<Flux<LLKeyScore>> results;

	public LLSearchResult(Mono<Long> totalHitsCount, Flux<Flux<LLKeyScore>> results) {
		this.totalHitsCount = totalHitsCount;
		this.results = results;
	}

	public static LLSearchResult empty() {
		return new LLSearchResult(Mono.just(0L), Flux.just(Flux.empty()));
	}

	@NotNull
	public static BiFunction<LLSearchResult, LLSearchResult, LLSearchResult> accumulator() {
		return (a, b) -> {
			var mergedTotals = a.totalHitsCount.flatMap(aL -> b.totalHitsCount.map(bL -> aL + bL));
			var mergedResults = Flux.merge(a.results, b.results);

			return new LLSearchResult(mergedTotals, mergedResults);
		};
	}

	public Mono<Long> totalHitsCount() {
		return this.totalHitsCount;
	}

	public Flux<Flux<LLKeyScore>> results() {
		return this.results;
	}

	public Mono<Void> completion() {
		return results.flatMap(r -> r).then();
	}

	public boolean equals(final Object o) {
		if (o == this) {
			return true;
		}
		if (!(o instanceof LLSearchResult)) {
			return false;
		}
		final LLSearchResult other = (LLSearchResult) o;
		final Object this$totalHitsCount = this.totalHitsCount();
		final Object other$totalHitsCount = other.totalHitsCount();
		if (!Objects.equals(this$totalHitsCount, other$totalHitsCount)) {
			return false;
		}
		final Object this$results = this.results();
		final Object other$results = other.results();
		return Objects.equals(this$results, other$results);
	}

	public int hashCode() {
		final int PRIME = 59;
		int result = 1;
		final Object $totalHitsCount = this.totalHitsCount();
		result = result * PRIME + ($totalHitsCount == null ? 43 : $totalHitsCount.hashCode());
		final Object $results = this.results();
		result = result * PRIME + ($results == null ? 43 : $results.hashCode());
		return result;
	}

	public String toString() {
		return "LLSearchResult(totalHitsCount=" + this.totalHitsCount() + ", results=" + this.results() + ")";
	}
}
