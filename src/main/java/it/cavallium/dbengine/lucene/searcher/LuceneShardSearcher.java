package it.cavallium.dbengine.lucene.searcher;

import it.cavallium.dbengine.client.query.current.data.QueryParams;
import org.apache.lucene.search.IndexSearcher;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

public interface LuceneShardSearcher {

	/**
	 * @param indexSearcher the index searcher, which contains all the lucene data
	 * @param queryParams the query parameters
	 * @param scheduler a blocking scheduler
	 */
	Mono<Void> searchOn(IndexSearcher indexSearcher,
			QueryParams queryParams,
			Scheduler scheduler);

	/**
	 * @param queryParams the query parameters
	 * @param keyFieldName the name of the key field
	 * @param scheduler a blocking scheduler
	 */
	Mono<LuceneSearchResult> collect(QueryParams queryParams, String keyFieldName, Scheduler scheduler);
}
