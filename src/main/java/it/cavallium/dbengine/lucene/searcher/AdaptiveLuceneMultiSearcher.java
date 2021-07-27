package it.cavallium.dbengine.lucene.searcher;

import reactor.core.publisher.Mono;

public class AdaptiveLuceneMultiSearcher implements LuceneMultiSearcher {

	private static final LuceneMultiSearcher scoredLuceneMultiSearcher = new ScoredLuceneMultiSearcher();

	private static final LuceneMultiSearcher unscoredPagedLuceneMultiSearcher = new UnscoredPagedLuceneMultiSearcher();

	private static final LuceneMultiSearcher unscoredIterableLuceneMultiSearcher = new UnscoredUnsortedContinuousLuceneMultiSearcher();

	private static final LuceneMultiSearcher countLuceneMultiSearcher = new CountLuceneMultiSearcher();

	@Override
	public Mono<LuceneShardSearcher> createShardSearcher(LocalQueryParams queryParams) {
		if (queryParams.limit() <= 0) {
			return countLuceneMultiSearcher.createShardSearcher(queryParams);
		} else if (queryParams.isScored()) {
			return scoredLuceneMultiSearcher.createShardSearcher(queryParams);
		} else if (queryParams.offset() == 0 && queryParams.limit() >= 2147483630 && !queryParams.isSorted()) {
			return unscoredIterableLuceneMultiSearcher.createShardSearcher(queryParams);
		} else {
			return unscoredPagedLuceneMultiSearcher.createShardSearcher(queryParams);
		}
	}
}
