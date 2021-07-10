package it.cavallium.dbengine.lucene.searcher;

import it.cavallium.dbengine.client.query.current.data.QueryParams;
import org.apache.lucene.search.IndexSearcher;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

public interface LuceneLocalSearcher {

	/**
	 * @param indexSearcher Lucene index searcher
	 * @param queryParams   the query parameters
	 * @param keyFieldName  the name of the key field
	 * @param scheduler     a blocking scheduler
	 */
	Mono<LuceneSearchResult> collect(IndexSearcher indexSearcher,
			Mono<Void> releaseIndexSearcher,
			LocalQueryParams queryParams,
			String keyFieldName,
			Scheduler scheduler);
}
