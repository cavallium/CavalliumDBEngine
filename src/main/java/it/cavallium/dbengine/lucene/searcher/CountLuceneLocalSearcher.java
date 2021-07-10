package it.cavallium.dbengine.lucene.searcher;

import it.cavallium.dbengine.client.query.QueryParser;
import it.cavallium.dbengine.client.query.current.data.QueryParams;
import org.apache.lucene.search.IndexSearcher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

public class CountLuceneLocalSearcher implements LuceneLocalSearcher {

	@Override
	public Mono<LuceneSearchResult> collect(IndexSearcher indexSearcher,
			Mono<Void> releaseIndexSearcher,
			LocalQueryParams queryParams,
			String keyFieldName,
			Scheduler scheduler) {
		//noinspection BlockingMethodInNonBlockingContext
		return Mono
				.fromCallable(() -> new LuceneSearchResult(
						indexSearcher.count(queryParams.query()),
						Flux.empty(),
						releaseIndexSearcher)
				)
				.subscribeOn(scheduler);
	}
}
