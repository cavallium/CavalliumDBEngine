package it.cavallium.dbengine.database;

import java.util.function.BiFunction;
import lombok.Value;
import org.jetbrains.annotations.NotNull;
import reactor.core.publisher.Flux;

@Value
public class LLSearchResult {

	Flux<LLSearchResultShard> results;

	@NotNull
	public static BiFunction<LLSearchResult, LLSearchResult, LLSearchResult> accumulator() {
		return (a, b) -> new LLSearchResult(Flux.merge(a.results, b.results));
	}
}
