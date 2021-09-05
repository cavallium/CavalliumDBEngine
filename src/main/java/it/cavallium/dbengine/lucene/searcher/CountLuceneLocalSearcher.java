package it.cavallium.dbengine.lucene.searcher;

import it.cavallium.dbengine.client.query.QueryParser;
import it.cavallium.dbengine.client.query.current.data.QueryParams;
import it.cavallium.dbengine.client.query.current.data.TotalHitsCount;
import org.apache.lucene.search.IndexSearcher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class CountLuceneLocalSearcher implements LuceneLocalSearcher {

	@Override
	public Mono<LuceneSearchResult> collect(IndexSearcher indexSearcher,
			Mono<Void> releaseIndexSearcher,
			LocalQueryParams queryParams,
			String keyFieldName) {
		return Mono
				.fromCallable(() -> {
					if (Schedulers.isInNonBlockingThread()) {
						throw new UnsupportedOperationException("Called collect in a nonblocking thread");
					}
					return new LuceneSearchResult(
									TotalHitsCount.of(indexSearcher.count(queryParams.query()), true),
									Flux.empty(),
									releaseIndexSearcher);
						}
				);
	}
}
