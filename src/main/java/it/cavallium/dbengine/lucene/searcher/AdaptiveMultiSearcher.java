package it.cavallium.dbengine.lucene.searcher;

import io.net5.buffer.api.Send;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.disk.LLIndexSearchers;
import it.cavallium.dbengine.database.disk.LLTempLMDBEnv;
import it.cavallium.dbengine.lucene.searcher.LLSearchTransformer.TransformerInput;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Mono;

public class AdaptiveMultiSearcher implements MultiSearcher {

	private static final OfficialSearcher officialSearcher = new OfficialSearcher();

	private static final MultiSearcher count = new CountMultiSearcher();

	private static final MultiSearcher scoredPaged = new ScoredPagedMultiSearcher();

	private static final MultiSearcher unsortedUnscoredContinuous = new UnsortedUnscoredStreamingMultiSearcher();

	/**
	 * Use in-memory collectors if the expected results count is lower or equal than this limit
	 */
	private final int maxInMemoryResultEntries;

	@Nullable
	private final UnsortedScoredFullMultiSearcher unsortedScoredFull;

	@Nullable
	private final SortedScoredFullMultiSearcher sortedScoredFull;

	public AdaptiveMultiSearcher(LLTempLMDBEnv env, boolean useLMDB, int maxInMemoryResultEntries) {
		unsortedScoredFull = useLMDB ? new UnsortedScoredFullMultiSearcher(env) : null;
		sortedScoredFull = useLMDB ?  new SortedScoredFullMultiSearcher(env) : null;
		this.maxInMemoryResultEntries = maxInMemoryResultEntries;
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
				= Math.max(maxInMemoryResultEntries, (long) queryParams.pageLimits().getPageLimit(0));

		return LLUtils.usingSendResource(indexSearchersMono, indexSearchers -> {
			if (queryParams.limitLong() == 0) {
				return count.collectMulti(indexSearchersMono, queryParams, keyFieldName, transformer);
			} else if (queryParams.isSorted() || queryParams.needsScores()) {
				if (realLimit <= maxAllowedInMemoryLimit) {
					return scoredPaged.collectMulti(indexSearchersMono, queryParams, keyFieldName, transformer);
				} else {
					if ((queryParams.isSorted() && !queryParams.isSortedByScore())) {
						if (queryParams.limitLong() < maxInMemoryResultEntries) {
							throw new UnsupportedOperationException("Allowed limit is " + maxInMemoryResultEntries + " or greater");
						}
						if (sortedScoredFull != null) {
							return sortedScoredFull.collectMulti(indexSearchersMono, queryParams, keyFieldName, transformer);
						} else {
							return scoredPaged.collectMulti(indexSearchersMono, queryParams, keyFieldName, transformer);
						}
					} else {
						if (queryParams.limitLong() < maxInMemoryResultEntries) {
							throw new UnsupportedOperationException("Allowed limit is " + maxInMemoryResultEntries + " or greater");
						}
						if (unsortedScoredFull != null) {
							return unsortedScoredFull.collectMulti(indexSearchersMono, queryParams, keyFieldName, transformer);
						} else {
							return scoredPaged.collectMulti(indexSearchersMono, queryParams, keyFieldName, transformer);
						}
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
