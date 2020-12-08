package it.cavallium.dbengine.database;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

public interface LLLuceneIndex extends Closeable, LLSnapshottable {

	String getLuceneIndexName();

	void addDocument(LLTerm id, LLDocument doc) throws IOException;

	void addDocuments(Iterable<LLTerm> keys, Iterable<LLDocument> documents) throws IOException;

	void deleteDocument(LLTerm id) throws IOException;

	void updateDocument(LLTerm id, LLDocument document) throws IOException;

	void updateDocuments(Iterable<LLTerm> ids, Iterable<LLDocument> documents) throws IOException;

	void deleteAll() throws IOException;

	Collection<LLTopKeys> search(@Nullable LLSnapshot snapshot, String query, int limit, @Nullable LLSort sort, String keyFieldName)
			throws IOException;

	Collection<LLTopKeys> moreLikeThis(@Nullable LLSnapshot snapshot,
			Map<String, Set<String>> mltDocumentFields,
			int limit,
			String keyFieldName) throws IOException;

	/**
	 *
	 * @param limit the limit is valid for each lucene instance.
	 *               If you have 15 instances, the number of elements returned
	 *               can be at most <code>limit * 15</code>
	 * @return the collection has one or more flux
	 */
	Tuple2<Mono<Long>, Collection<Flux<String>>> searchStream(@Nullable LLSnapshot snapshot,
			String query,
			int limit,
			@Nullable LLSort sort,
			String keyFieldName);

	long count(@Nullable LLSnapshot snapshot, String query) throws IOException;

	boolean isLowMemoryMode();
}
