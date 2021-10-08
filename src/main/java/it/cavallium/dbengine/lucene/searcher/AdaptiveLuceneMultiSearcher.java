package it.cavallium.dbengine.lucene.searcher;

import io.net5.buffer.api.Send;
import it.cavallium.dbengine.database.disk.LLIndexSearchers;
import reactor.core.publisher.Mono;

public class AdaptiveLuceneMultiSearcher implements LuceneMultiSearcher {

	private static final LuceneMultiSearcher countLuceneMultiSearcher
			= new SimpleUnsortedUnscoredLuceneMultiSearcher(new CountLuceneLocalSearcher());

	private static final LuceneMultiSearcher scoredSimpleLuceneShardSearcher
			= new ScoredSimpleLuceneShardSearcher();

	private static final LuceneMultiSearcher unsortedUnscoredPagedLuceneMultiSearcher
			= new SimpleUnsortedUnscoredLuceneMultiSearcher(new SimpleLuceneLocalSearcher());

	private static final LuceneMultiSearcher unsortedUnscoredContinuousLuceneMultiSearcher
			= new UnsortedUnscoredContinuousLuceneMultiSearcher();

	//todo: detect transformed query params, not input query params!
	@Override
	public Mono<Send<LuceneSearchResult>> collectMulti(Mono<Send<LLIndexSearchers>> indexSearchersMono,
			LocalQueryParams queryParams,
			String keyFieldName,
			LLSearchTransformer transformer) {
		if (queryParams.limit() == 0) {
			return countLuceneMultiSearcher.collectMulti(indexSearchersMono, queryParams, keyFieldName, transformer);
		} else if (queryParams.isSorted() || queryParams.isScored()) {
			return scoredSimpleLuceneShardSearcher.collectMulti(indexSearchersMono, queryParams, keyFieldName, transformer);
		} else {
			if (((long) queryParams.offset() + (long) queryParams.limit()) <= (long) queryParams.pageLimits().getPageLimit(0)
					|| transformer != null) {
				// Run single-page searches using the paged multi searcher
				return unsortedUnscoredPagedLuceneMultiSearcher.collectMulti(indexSearchersMono, queryParams, keyFieldName, transformer);
			} else {
				// Run large/unbounded searches using the continuous multi searcher
				return unsortedUnscoredContinuousLuceneMultiSearcher.collectMulti(indexSearchersMono, queryParams, keyFieldName, transformer);
			}
		}
	}
}
