package it.cavallium.dbengine.client;

import org.jetbrains.annotations.NotNull;
import reactor.core.publisher.Mono;

public record SearchResultItem<T, U>(Mono<T> key, Mono<U> value, float score)
		implements Comparable<SearchResultItem<T, U>> {

	@Override
	public int compareTo(@NotNull SearchResultItem<T, U> o) {
		return Float.compare(o.score, this.score);
	}
}
