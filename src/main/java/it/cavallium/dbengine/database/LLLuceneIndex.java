package it.cavallium.dbengine.database;

import java.util.Map;
import java.util.Set;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Mono;

public interface LLLuceneIndex extends LLSnapshottable {

	String getLuceneIndexName();

	Mono<Void> addDocument(LLTerm id, LLDocument doc);

	Mono<Void> addDocuments(Iterable<LLTerm> keys, Iterable<LLDocument> documents);

	Mono<Void> deleteDocument(LLTerm id);

	Mono<Void> updateDocument(LLTerm id, LLDocument document);

	Mono<Void> updateDocuments(Iterable<LLTerm> ids, Iterable<LLDocument> documents);

	Mono<Void> deleteAll();

	/**
	 *
	 * @param limit the limit is valid for each lucene instance.
	 *               If you have 15 instances, the number of elements returned
	 *               can be at most <code>limit * 15</code>
	 * @return the collection has one or more flux
	 */
	Mono<LLSearchResult> moreLikeThis(@Nullable LLSnapshot snapshot,
			Map<String, Set<String>> mltDocumentFields,
			int limit,
			String keyFieldName);

	/**
	 *
	 * @param limit the limit is valid for each lucene instance.
	 *               If you have 15 instances, the number of elements returned
	 *               can be at most <code>limit * 15</code>
	 * @return the collection has one or more flux
	 */
	Mono<LLSearchResult> search(@Nullable LLSnapshot snapshot,
			String query,
			int limit,
			@Nullable LLSort sort,
			LLScoreMode scoreMode,
			String keyFieldName);

	default Mono<Long> count(@Nullable LLSnapshot snapshot, String queryString) {
		return this.search(snapshot, queryString, 0, null, null, null)
				.flatMap(LLSearchResult::totalHitsCount)
				.single();
	}

	boolean isLowMemoryMode();

	Mono<Void> close();
}
