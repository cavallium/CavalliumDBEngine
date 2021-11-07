package it.cavallium.dbengine.client;

import it.cavallium.dbengine.database.LLIndexRequest;
import it.cavallium.dbengine.database.LLUpdateDocument;
import it.cavallium.dbengine.database.LLTerm;
import java.util.Set;
import org.jetbrains.annotations.NotNull;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

public abstract class Indicizer<T, U> {

	/**
	 * Transform a value to an IndexRequest.
	 */
	public abstract @NotNull Mono<? extends LLIndexRequest> toIndexRequest(@NotNull T key, @NotNull U value);

	public abstract @NotNull LLTerm toIndex(@NotNull T key);

	public abstract @NotNull String getKeyFieldName();

	public abstract @NotNull T getKey(String key);

	public abstract IndicizerAnalyzers getPerFieldAnalyzer();

	public abstract IndicizerSimilarities getPerFieldSimilarity();

	public Flux<Tuple2<String, Set<String>>> getMoreLikeThisDocumentFields(T key, U value) {
		return Flux.empty();
	}
}
