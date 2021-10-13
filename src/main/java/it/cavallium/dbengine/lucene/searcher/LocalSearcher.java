package it.cavallium.dbengine.lucene.searcher;

import io.net5.buffer.api.Send;
import it.cavallium.dbengine.database.disk.LLIndexSearcher;
import reactor.core.publisher.Mono;

public interface LocalSearcher {

	/**
	 * @param indexSearcherMono Lucene index searcher
	 * @param queryParams       the query parameters
	 * @param keyFieldName      the name of the key field
	 * @param transformer       the search query transformer
	 */
	Mono<Send<LuceneSearchResult>> collect(Mono<Send<LLIndexSearcher>> indexSearcherMono,
			LocalQueryParams queryParams,
			String keyFieldName,
			LLSearchTransformer transformer);

	/**
	 * Get the name of this searcher type
	 * @return searcher type name
	 */
	String getName();
}
