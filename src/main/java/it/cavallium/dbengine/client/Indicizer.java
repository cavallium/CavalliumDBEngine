package it.cavallium.dbengine.client;

import it.cavallium.dbengine.database.LLDocument;
import it.cavallium.dbengine.database.LLTerm;
import java.util.Set;
import org.jetbrains.annotations.NotNull;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

public abstract class Indicizer<T, U> {

	public abstract @NotNull Mono<LLDocument> toDocument(@NotNull T key, @NotNull U value);

	public abstract @NotNull LLTerm toIndex(@NotNull T key);

	public abstract @NotNull String getKeyFieldName();

	public abstract @NotNull T getKey(String key);

	public Flux<Tuple2<String, Set<String>>> getMoreLikeThisDocumentFields(T key, U value) {
		return Flux.empty();
	}
}
