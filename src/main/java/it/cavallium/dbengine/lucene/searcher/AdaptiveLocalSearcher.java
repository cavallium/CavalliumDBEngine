package it.cavallium.dbengine.lucene.searcher;

import io.net5.buffer.api.Send;
import io.net5.buffer.api.internal.ResourceSupport;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.disk.LLIndexSearcher;
import it.cavallium.dbengine.database.disk.LLIndexSearchers;
import it.cavallium.dbengine.database.disk.LLTempLMDBEnv;
import it.cavallium.dbengine.lucene.searcher.LLSearchTransformer.TransformerInput;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Mono;

public class AdaptiveLocalSearcher implements LocalSearcher {

	private static final OfficialSearcher officialSearcher = new OfficialSearcher();

	private static final LocalSearcher scoredPaged = new PagedLocalSearcher();

	private static final LocalSearcher countSearcher = new CountMultiSearcher();

	private static final MultiSearcher unsortedUnscoredContinuous = new UnsortedUnscoredStreamingMultiSearcher();

	/**
	 * Use in-memory collectors if the expected results count is lower or equal than this limit
	 */
	private final int maxInMemoryResultEntries;

	@Nullable
	private final UnsortedScoredFullMultiSearcher unsortedScoredFull;

	@Nullable
	private final SortedScoredFullMultiSearcher sortedScoredFull;

	public AdaptiveLocalSearcher(LLTempLMDBEnv env, boolean useLMDB, int maxInMemoryResultEntries) {
		unsortedScoredFull = useLMDB ? new UnsortedScoredFullMultiSearcher(env) : null;
		sortedScoredFull = useLMDB ? new SortedScoredFullMultiSearcher(env) : null;
		this.maxInMemoryResultEntries = maxInMemoryResultEntries;
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
				= Math.max(maxInMemoryResultEntries, (long) queryParams.pageLimits().getPageLimit(0));

		if (queryParams.limitLong() == 0) {
			return countSearcher.collect(indexSearcher, queryParams, keyFieldName, transformer);
		} else if (queryParams.isSorted() || queryParams.needsScores()) {
			if (realLimit <= maxAllowedInMemoryLimit) {
				return scoredPaged.collect(indexSearcher, queryParams, keyFieldName, transformer);
			} else {
				if ((queryParams.isSorted() && !queryParams.isSortedByScore())) {
					if (queryParams.limitLong() < maxInMemoryResultEntries) {
						throw new UnsupportedOperationException("Allowed limit is " + maxInMemoryResultEntries + " or greater");
					}
					if (sortedScoredFull != null) {
						return sortedScoredFull.collect(indexSearcher, queryParams, keyFieldName, transformer);
					} else {
						return scoredPaged.collect(indexSearcher, queryParams, keyFieldName, transformer);
					}
				} else {
					if (queryParams.limitLong() < maxInMemoryResultEntries) {
						throw new UnsupportedOperationException("Allowed limit is " + maxInMemoryResultEntries + " or greater");
					}
					if (unsortedScoredFull != null) {
						return unsortedScoredFull.collect(indexSearcher, queryParams, keyFieldName, transformer);
					} else {
						return scoredPaged.collect(indexSearcher, queryParams, keyFieldName, transformer);
					}
				}
			}
		} else {
			// Run large/unbounded searches using the continuous multi searcher
			return unsortedUnscoredContinuous.collect(indexSearcher, queryParams, keyFieldName, transformer);
		}
	}
}
