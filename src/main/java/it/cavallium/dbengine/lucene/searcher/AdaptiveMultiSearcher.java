package it.cavallium.dbengine.lucene.searcher;

import io.net5.buffer.api.Send;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.disk.LLIndexSearchers;
import it.cavallium.dbengine.database.disk.LLTempLMDBEnv;
import it.cavallium.dbengine.lucene.searcher.LLSearchTransformer.TransformerInput;
import reactor.core.publisher.Mono;

public class AdaptiveMultiSearcher implements MultiSearcher {

	private static final MultiSearcher count = new CountMultiSearcher();

	private static final MultiSearcher scoredPaged = new ScoredPagedMultiSearcher();

	private static final MultiSearcher unsortedUnscoredContinuous = new UnsortedUnscoredStreamingMultiSearcher();

	private final UnsortedScoredFullMultiSearcher unsortedScoredFull;

	private final SortedScoredFullMultiSearcher sortedScoredFull;

	public AdaptiveMultiSearcher(LLTempLMDBEnv env) {
		unsortedScoredFull = new UnsortedScoredFullMultiSearcher(env);
		sortedScoredFull = new SortedScoredFullMultiSearcher(env);
	}

	@Override
	public Mono<LuceneSearchResult> collectMulti(Mono<Send<LLIndexSearchers>> indexSearchersMono,
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

	// Remember to change also AdaptiveLocalSearcher
	public Mono<LuceneSearchResult> transformedCollectMulti(Mono<Send<LLIndexSearchers>> indexSearchersMono,
			LocalQueryParams queryParams,
			String keyFieldName,
			LLSearchTransformer transformer) {
		// offset + limit
		long realLimit = queryParams.offsetLong() + queryParams.limitLong();
		long maxAllowedInMemoryLimit
				= Math.max(MultiSearcher.MAX_IN_MEMORY_SIZE, (long) queryParams.pageLimits().getPageLimit(0));

		return LLUtils.usingSendResource(indexSearchersMono, indexSearchers -> {
			if (queryParams.limitLong() == 0) {
				return count.collectMulti(indexSearchersMono, queryParams, keyFieldName, transformer);
			} else if (queryParams.isSorted() || queryParams.needsScores()) {
				if (realLimit <= maxAllowedInMemoryLimit) {
					return scoredPaged.collectMulti(indexSearchersMono, queryParams, keyFieldName, transformer);
				} else {
					if ((queryParams.isSorted() && !queryParams.isSortedByScore())) {
						if (queryParams.limitLong() < MAX_IN_MEMORY_SIZE) {
							throw new UnsupportedOperationException("Allowed limit is " + MAX_IN_MEMORY_SIZE + " or greater");
						}
						return sortedScoredFull.collectMulti(indexSearchersMono, queryParams, keyFieldName, transformer);
					} else {
						if (queryParams.limitLong() < MAX_IN_MEMORY_SIZE) {
							throw new UnsupportedOperationException("Allowed limit is " + MAX_IN_MEMORY_SIZE + " or greater");
						}
						return unsortedScoredFull.collectMulti(indexSearchersMono, queryParams, keyFieldName, transformer);
					}
				}
			} else {
				// Run large/unbounded searches using the continuous multi searcher
				return unsortedUnscoredContinuous.collectMulti(indexSearchersMono, queryParams, keyFieldName, transformer);
			}
		}, true);
	}

	@Override
	public String getName() {
		return "adaptive local";
	}
}
