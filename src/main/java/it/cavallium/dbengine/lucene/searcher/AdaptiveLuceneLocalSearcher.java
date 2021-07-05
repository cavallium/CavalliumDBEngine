package it.cavallium.dbengine.lucene.searcher;

import it.cavallium.dbengine.client.query.current.data.QueryParams;
import org.apache.lucene.search.IndexSearcher;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

public class AdaptiveLuceneLocalSearcher implements LuceneLocalSearcher {

	private static final LuceneLocalSearcher localSearcher = new SimpleLuceneLocalSearcher();

	private static final LuceneLocalSearcher countSearcher = new CountLuceneLocalSearcher();

	@Override
	public Mono<LuceneSearchResult> collect(IndexSearcher indexSearcher,
			LocalQueryParams queryParams,
			String keyFieldName,
			Scheduler scheduler) {
		if (queryParams.limit() == 0) {
			return countSearcher.collect(indexSearcher, queryParams, keyFieldName, scheduler);
		} else {
			return localSearcher.collect(indexSearcher, queryParams, keyFieldName, scheduler);
		}
	}
}
