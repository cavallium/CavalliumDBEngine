package it.cavallium.dbengine.database;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Mono;

public interface LLLuceneIndex extends Closeable, LLSnapshottable {

	String getLuceneIndexName();

	void addDocument(LLTerm id, LLDocument doc) throws IOException;

	void addDocuments(Iterable<LLTerm> keys, Iterable<LLDocument> documents) throws IOException;

	void deleteDocument(LLTerm id) throws IOException;

	void updateDocument(LLTerm id, LLDocument document) throws IOException;

	void updateDocuments(Iterable<LLTerm> ids, Iterable<LLDocument> documents) throws IOException;

	void deleteAll() throws IOException;

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

	long count(@Nullable LLSnapshot snapshot, String query) throws IOException;

	boolean isLowMemoryMode();
}
