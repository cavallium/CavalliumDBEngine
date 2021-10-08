package it.cavallium.dbengine.lucene.searcher;

import io.net5.buffer.api.Send;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.disk.LLIndexSearchers;
import it.cavallium.dbengine.lucene.searcher.LLSearchTransformer.TransformerInput;
import reactor.core.publisher.Mono;

public class AdaptiveLuceneMultiSearcher implements LuceneMultiSearcher {

	private static final LuceneMultiSearcher count
			= new SimpleUnsortedUnscoredLuceneMultiSearcher(new CountLuceneLocalSearcher());

	private static final LuceneMultiSearcher scoredSimple
			= new ScoredSimpleLuceneShardSearcher();

	private static final LuceneMultiSearcher unsortedUnscoredPaged
			= new SimpleUnsortedUnscoredLuceneMultiSearcher(new SimpleLuceneLocalSearcher());

	private static final LuceneMultiSearcher unsortedUnscoredContinuous
			= new UnsortedUnscoredContinuousLuceneMultiSearcher();

	@Override
	public Mono<Send<LuceneSearchResult>> collectMulti(Mono<Send<LLIndexSearchers>> indexSearchersMono,
			LocalQueryParams queryParams,
			String keyFieldName,
			LLSearchTransformer transformer) {
		if (transformer == LLSearchTransformer.NO_TRANSFORMATION) {
			return transformedCollectMulti(indexSearchersMono, queryParams, keyFieldName, transformer);
		} else {
			return LLUtils.usingSendResource(indexSearchersMono, indexSearchers -> transformer
					.transform(Mono.fromCallable(() -> new TransformerInput(indexSearchers, queryParams)))
					.flatMap(queryParams2 -> this
							.transformedCollectMulti(indexSearchersMono, queryParams2, keyFieldName, LLSearchTransformer.NO_TRANSFORMATION)),
					true);
		}
	}

	public Mono<Send<LuceneSearchResult>> transformedCollectMulti(Mono<Send<LLIndexSearchers>> indexSearchersMono,
			LocalQueryParams queryParams,
			String keyFieldName,
			LLSearchTransformer transformer) {
		// offset + limit
		long realLimit = ((long) queryParams.offset() + (long) queryParams.limit());

		return LLUtils.usingSendResource(indexSearchersMono, indexSearchers -> {
			if (queryParams.limit() == 0) {
				return count.collectMulti(indexSearchersMono, queryParams, keyFieldName, transformer);
			} else if (queryParams.isSorted() || queryParams.isScored()) {
				return scoredSimple.collectMulti(indexSearchersMono, queryParams, keyFieldName, transformer);
			} else if (realLimit <= (long) queryParams.pageLimits().getPageLimit(0)) {
				// Run single-page searches using the paged multi searcher
				return unsortedUnscoredPaged.collectMulti(indexSearchersMono, queryParams, keyFieldName, transformer);
			} else {
				// Run large/unbounded searches using the continuous multi searcher
				return unsortedUnscoredContinuous.collectMulti(indexSearchersMono, queryParams, keyFieldName, transformer);
			}
		}, true);
	}
}
