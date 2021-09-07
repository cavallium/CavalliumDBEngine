package it.cavallium.dbengine.lucene.searcher;

import org.apache.lucene.search.IndexSearcher;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class AdaptiveLuceneLocalSearcher implements LuceneLocalSearcher {

	private static final LuceneLocalSearcher localSearcher = new SimpleLuceneLocalSearcher();

	private static final LuceneLocalSearcher unscoredPagedLuceneLocalSearcher = new LocalLuceneWrapper(new UnscoredUnsortedContinuousLuceneMultiSearcher());

	private static final LuceneLocalSearcher countSearcher = new CountLuceneLocalSearcher();

	@Override
	public Mono<LuceneSearchResult> collect(IndexSearcher indexSearcher,
			Mono<Void> releaseIndexSearcher,
			LocalQueryParams queryParams,
			String keyFieldName,
			Scheduler scheduler) {
		if (Schedulers.isInNonBlockingThread()) {
			return releaseIndexSearcher
					.then(Mono.error(() -> new UnsupportedOperationException("Called collect in a nonblocking thread")));
		}
		if (queryParams.limit() == 0) {
			return countSearcher.collect(indexSearcher, releaseIndexSearcher, queryParams, keyFieldName, scheduler);
		} else if (!queryParams.isScored() && queryParams.offset() == 0 && queryParams.limit() >= 2147483630
				&& !queryParams.isSorted()) {
			return unscoredPagedLuceneLocalSearcher.collect(indexSearcher,
					releaseIndexSearcher,
					queryParams,
					keyFieldName,
					scheduler
			);
		} else {
			return localSearcher.collect(indexSearcher, releaseIndexSearcher, queryParams, keyFieldName, scheduler);
		}
	}
}
