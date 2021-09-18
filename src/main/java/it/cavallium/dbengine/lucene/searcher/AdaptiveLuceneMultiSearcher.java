package it.cavallium.dbengine.lucene.searcher;

import io.net5.buffer.api.Send;
import it.cavallium.dbengine.database.LLUtils;
import reactor.core.publisher.Mono;

public class AdaptiveLuceneMultiSearcher implements LuceneMultiSearcher {

	private static final LuceneMultiSearcher scoredLuceneMultiSearcher = new ScoredLuceneMultiSearcher();

	private static final LuceneMultiSearcher unscoredPagedLuceneMultiSearcher = new UnscoredPagedLuceneMultiSearcher();

	private static final LuceneMultiSearcher unscoredIterableLuceneMultiSearcher = new UnscoredUnsortedContinuousLuceneMultiSearcher();

	private static final LuceneMultiSearcher countLuceneMultiSearcher = new CountLuceneMultiSearcher();

	@Override
	public Mono<Send<LuceneMultiSearcher>> createShardSearcher(LocalQueryParams queryParams) {
		Mono<Send<LuceneMultiSearcher>> shardSearcherCreationMono;
		if (queryParams.limit() <= 0) {
			shardSearcherCreationMono = countLuceneMultiSearcher.createShardSearcher(queryParams);
		} else if (queryParams.isScored()) {
			shardSearcherCreationMono = scoredLuceneMultiSearcher.createShardSearcher(queryParams);
		} else if (queryParams.offset() == 0 && queryParams.limit() >= 2147483630 && !queryParams.isSorted()) {
			shardSearcherCreationMono = unscoredIterableLuceneMultiSearcher.createShardSearcher(queryParams);
		} else {
			shardSearcherCreationMono = unscoredPagedLuceneMultiSearcher.createShardSearcher(queryParams);
		}
		return Mono.fromRunnable(LLUtils::ensureBlocking).then(shardSearcherCreationMono);
	}
}
