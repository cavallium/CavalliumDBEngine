package it.cavallium.dbengine.lucene.searcher;

import static it.cavallium.dbengine.lucene.searcher.MultiSearcher.MAX_IN_MEMORY_SIZE;

import io.net5.buffer.api.Send;
import io.net5.buffer.api.internal.ResourceSupport;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.disk.LLIndexSearcher;
import it.cavallium.dbengine.database.disk.LLIndexSearchers;
import it.cavallium.dbengine.database.disk.LLTempLMDBEnv;
import it.cavallium.dbengine.lucene.searcher.LLSearchTransformer.TransformerInput;
import reactor.core.publisher.Mono;

public class AdaptiveLocalSearcher implements LocalSearcher {

	private static final LocalSearcher localPagedSearcher = new PagedLocalSearcher();

	private static final LocalSearcher countSearcher = new CountMultiSearcher();

	private static final MultiSearcher unsortedUnscoredContinuous = new UnsortedUnscoredStreamingMultiSearcher();

	private final UnsortedScoredFullMultiSearcher unsortedScoredFull;

	private final SortedScoredFullMultiSearcher sortedScoredFull;

	public AdaptiveLocalSearcher(LLTempLMDBEnv env) {
		unsortedScoredFull = new UnsortedScoredFullMultiSearcher(env);
		sortedScoredFull = new SortedScoredFullMultiSearcher(env);
	}

	@Override
	public Mono<LuceneSearchResult> collect(Mono<Send<LLIndexSearcher>> indexSearcher,
			LocalQueryParams queryParams,
			String keyFieldName,
			LLSearchTransformer transformer) {
		Mono<Send<LLIndexSearchers>> indexSearchersMono = indexSearcher
				.map(LLIndexSearchers::unsharded)
				.map(ResourceSupport::send);

		if (transformer == LLSearchTransformer.NO_TRANSFORMATION) {
			return transformedCollect(indexSearcher, queryParams, keyFieldName, transformer);
		} else {
			return LLUtils.usingSendResource(indexSearchersMono, indexSearchers -> transformer
							.transform(Mono.fromCallable(() -> new TransformerInput(indexSearchers, queryParams)))
							.flatMap(queryParams2 -> this
									.transformedCollect(indexSearcher, queryParams2, keyFieldName, LLSearchTransformer.NO_TRANSFORMATION)),
					true);
		}
	}

	@Override
	public String getName() {
		return "adaptivelocal";
	}

	// Remember to change also AdaptiveMultiSearcher
	public Mono<LuceneSearchResult> transformedCollect(Mono<Send<LLIndexSearcher>> indexSearcher,
			LocalQueryParams queryParams,
			String keyFieldName,
			LLSearchTransformer transformer) {
		// offset + limit
		long realLimit = queryParams.offsetLong() + queryParams.limitLong();
		long maxAllowedInMemoryLimit
				= Math.max(MAX_IN_MEMORY_SIZE, (long) queryParams.pageLimits().getPageLimit(0));

		if (queryParams.limitLong() == 0) {
			return countSearcher.collect(indexSearcher, queryParams, keyFieldName, transformer);
		} else if (queryParams.isSorted() || queryParams.needsScores()) {
			if (realLimit <= maxAllowedInMemoryLimit) {
				return localPagedSearcher.collect(indexSearcher, queryParams, keyFieldName, transformer);
			} else {
				if ((queryParams.isSorted() && !queryParams.isSortedByScore())) {
					if (queryParams.limitLong() < MAX_IN_MEMORY_SIZE) {
						throw new UnsupportedOperationException("Allowed limit is " + MAX_IN_MEMORY_SIZE + " or greater");
					}
					return sortedScoredFull.collect(indexSearcher, queryParams, keyFieldName, transformer);
				} else {
					if (queryParams.limitLong() < MAX_IN_MEMORY_SIZE) {
						throw new UnsupportedOperationException("Allowed limit is " + MAX_IN_MEMORY_SIZE + " or greater");
					}
					return unsortedScoredFull.collect(indexSearcher, queryParams, keyFieldName, transformer);
				}
			}
		} else {
			// Run large/unbounded searches using the continuous multi searcher
			return unsortedUnscoredContinuous.collect(indexSearcher, queryParams, keyFieldName, transformer);
		}
	}
}
