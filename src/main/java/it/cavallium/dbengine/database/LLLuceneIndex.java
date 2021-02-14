package it.cavallium.dbengine.database;

import it.cavallium.dbengine.lucene.serializer.Query;
import java.util.Set;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

public interface LLLuceneIndex extends LLSnapshottable {

	String getLuceneIndexName();

	Mono<Void> addDocument(LLTerm id, LLDocument doc);

	Mono<Void> addDocuments(Flux<GroupedFlux<LLTerm, LLDocument>> documents);

	Mono<Void> deleteDocument(LLTerm id);

	Mono<Void> updateDocument(LLTerm id, LLDocument document);

	Mono<Void> updateDocuments(Flux<GroupedFlux<LLTerm, LLDocument>> documents);

	Mono<Void> deleteAll();

	/**
	 *
	 * @param limit the limit is valid for each lucene instance.
	 *               If you have 15 instances, the number of elements returned
	 *               can be at most <code>limit * 15</code>
	 * @return the collection has one or more flux
	 */
	Mono<LLSearchResult> moreLikeThis(@Nullable LLSnapshot snapshot,
			Flux<Tuple2<String, Set<String>>> mltDocumentFields,
			int limit,
			@Nullable Float minCompetitiveScore,
			String keyFieldName);

	/**
	 *
	 * @param limit the limit is valid for each lucene instance.
	 *               If you have 15 instances, the number of elements returned
	 *               can be at most <code>limit * 15</code>
	 * @return the collection has one or more flux
	 */
	Mono<LLSearchResult> search(@Nullable LLSnapshot snapshot,
			Query query,
			int limit,
			@Nullable LLSort sort,
			LLScoreMode scoreMode,
			@Nullable Float minCompetitiveScore,
			String keyFieldName);

	default Mono<Long> count(@Nullable LLSnapshot snapshot, Query query) {
		return this.search(snapshot, query, 0, null, null, null, null)
				.flatMap(LLSearchResult::totalHitsCount)
				.single();
	}

	boolean isLowMemoryMode();

	Mono<Void> close();

	/**
	 * Flush writes to disk
	 */
	Mono<Void> flush();

	/**
	 * Refresh index searcher
	 */
	Mono<Void> refresh();
}
