package it.cavallium.dbengine.database;

import java.util.Objects;
import java.util.function.BiFunction;
import org.jetbrains.annotations.NotNull;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class LLSearchResult {

	private final Flux<Flux<LLSignal>> results;

	public LLSearchResult(Flux<Flux<LLSignal>> results) {
		this.results = results;
	}

	public static LLSearchResult empty() {
		return new LLSearchResult(Flux.just(Flux.just(new LLTotalHitsCount(0L))));
	}

	@NotNull
	public static BiFunction<LLSearchResult, LLSearchResult, LLSearchResult> accumulator() {
		return (a, b) -> new LLSearchResult(Flux.merge(a.results, b.results));
	}

	public Flux<Flux<LLSignal>> results() {
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
		final Object this$results = this.results();
		final Object other$results = other.results();
		return Objects.equals(this$results, other$results);
	}

	public int hashCode() {
		final int PRIME = 59;
		int result = 1;
		final Object $results = this.results();
		result = result * PRIME + ($results == null ? 43 : $results.hashCode());
		return result;
	}

	public String toString() {
		return "LLSearchResult(results=" + this.results() + ")";
	}
}
