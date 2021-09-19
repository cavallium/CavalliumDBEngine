package it.cavallium.dbengine.lucene.searcher;

import io.net5.buffer.api.Send;
import it.cavallium.dbengine.database.disk.LLIndexContext;
import it.cavallium.dbengine.database.disk.LLIndexSearcher;
import reactor.core.publisher.Mono;

public interface LuceneLocalSearcher {

	/**
	 * @param indexSearcherMono Lucene index searcher
	 * @param queryParams   the query parameters
	 * @param keyFieldName  the name of the key field
	 */
	Mono<Send<LuceneSearchResult>> collect(Mono<Send<LLIndexContext>> indexSearcherMono,
			LocalQueryParams queryParams,
			String keyFieldName);
}
