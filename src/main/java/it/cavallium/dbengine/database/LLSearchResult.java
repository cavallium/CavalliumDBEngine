package it.cavallium.dbengine.database;

import java.util.function.BiFunction;
import java.util.stream.Stream;
import org.jetbrains.annotations.NotNull;

public record LLSearchResult(Stream<LLSearchResultShard> results) {

	@NotNull
	public static BiFunction<LLSearchResult, LLSearchResult, LLSearchResult> accumulator() {
		return (a, b) -> new LLSearchResult(Stream.concat(a.results, b.results));
	}
}
