package it.cavallium.dbengine.lucene.searcher;

import reactor.core.publisher.Mono;

public class AdaptiveLuceneMultiSearcher implements LuceneMultiSearcher {

	private static final LuceneMultiSearcher scoredLuceneMultiSearcher = new ScoredLuceneMultiSearcher();

	private static final LuceneMultiSearcher unscoredLuceneMultiSearcher = new UnscoredLuceneMultiSearcher();

	private static final LuceneMultiSearcher countLuceneMultiSearcher = new CountLuceneMultiSearcher();

	@Override
	public Mono<LuceneShardSearcher> createShardSearcher(LocalQueryParams queryParams) {
		if (queryParams.limit() <= 0) {
			return countLuceneMultiSearcher.createShardSearcher(queryParams);
		} else if (queryParams.isScored()) {
			return scoredLuceneMultiSearcher.createShardSearcher(queryParams);
		} else {
			return unscoredLuceneMultiSearcher.createShardSearcher(queryParams);
		}
	}
}
