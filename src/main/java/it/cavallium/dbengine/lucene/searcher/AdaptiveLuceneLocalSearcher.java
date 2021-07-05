package it.cavallium.dbengine.lucene.searcher;

import it.cavallium.dbengine.client.query.current.data.QueryParams;
import org.apache.lucene.search.IndexSearcher;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

public class AdaptiveLuceneLocalSearcher implements LuceneLocalSearcher {

	private static final LuceneLocalSearcher localSearcher = new SimpleLuceneLocalSearcher();

	@Override
	public Mono<LuceneSearchResult> collect(IndexSearcher indexSearcher,
			QueryParams queryParams,
			String keyFieldName,
			Scheduler scheduler) {
		return localSearcher.collect(indexSearcher, queryParams, keyFieldName, scheduler);
	}
}
