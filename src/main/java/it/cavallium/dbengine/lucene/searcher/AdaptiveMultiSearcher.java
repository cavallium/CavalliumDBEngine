package it.cavallium.dbengine.lucene.searcher;

import static it.cavallium.dbengine.client.UninterruptibleScheduler.uninterruptibleScheduler;
import static it.cavallium.dbengine.database.LLUtils.singleOrClose;
import static it.cavallium.dbengine.lucene.searcher.AdaptiveLocalSearcher.FORCE_HUGE_PQ;
import static it.cavallium.dbengine.lucene.searcher.GlobalQueryRewrite.NO_REWRITE;

import io.netty5.util.Send;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.disk.LLIndexSearchers;
import it.cavallium.dbengine.database.disk.LLTempHugePqEnv;
import it.cavallium.dbengine.lucene.LuceneUtils;
import java.io.IOException;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class AdaptiveMultiSearcher implements MultiSearcher {

	private static final StandardSearcher standardSearcher = new StandardSearcher();

	private static final MultiSearcher count = new CountMultiSearcher();

	private static final MultiSearcher scoredPaged = new ScoredPagedMultiSearcher();

	private static final MultiSearcher unsortedUnscoredContinuous = new UnsortedStreamingMultiSearcher();

	/**
	 * Use in-memory collectors if the expected results count is lower or equal than this limit
	 */
	private final int maxInMemoryResultEntries;

	@Nullable
	private final SortedByScoreFullMultiSearcher sortedByScoreFull;

	@Nullable
	private final SortedScoredFullMultiSearcher sortedScoredFull;

	public AdaptiveMultiSearcher(LLTempHugePqEnv env, boolean useHugePq, int maxInMemoryResultEntries) {
		sortedByScoreFull = (FORCE_HUGE_PQ || useHugePq) ? new SortedByScoreFullMultiSearcher(env) : null;
		sortedScoredFull = (FORCE_HUGE_PQ || useHugePq) ?  new SortedScoredFullMultiSearcher(env) : null;
		this.maxInMemoryResultEntries = maxInMemoryResultEntries;
	}

	@Override
	public Mono<LuceneSearchResult> collectMulti(Mono<LLIndexSearchers> indexSearchersMono,
			LocalQueryParams queryParams,
			@Nullable String keyFieldName,
			GlobalQueryRewrite transformer) {
		if (transformer != NO_REWRITE) {
			return LuceneUtils.rewriteMulti(this, indexSearchersMono, queryParams, keyFieldName, transformer);
		}
		return transformedCollectMulti(indexSearchersMono, queryParams, keyFieldName, transformer);
	}

	// Remember to change also AdaptiveLocalSearcher
	public Mono<LuceneSearchResult> transformedCollectMulti(Mono<LLIndexSearchers> indexSearchers,
			LocalQueryParams queryParams,
			@Nullable String keyFieldName,
			GlobalQueryRewrite transformer) {
		// offset + limit
		long realLimit = queryParams.offsetLong() + queryParams.limitLong();
		long maxAllowedInMemoryLimit
				= Math.max(maxInMemoryResultEntries, (long) queryParams.pageLimits().getPageLimit(0));

		if (!FORCE_HUGE_PQ && queryParams.limitLong() == 0) {
			return count.collectMulti(indexSearchers, queryParams, keyFieldName, transformer);
		} else if (!FORCE_HUGE_PQ && realLimit <= maxInMemoryResultEntries) {
			return standardSearcher.collectMulti(indexSearchers, queryParams, keyFieldName, transformer);
		} else if (queryParams.isSorted()) {
			if (!FORCE_HUGE_PQ && realLimit <= maxAllowedInMemoryLimit) {
				return scoredPaged.collectMulti(indexSearchers, queryParams, keyFieldName, transformer);
			} else {
				if (queryParams.isSortedByScore()) {
					if (!FORCE_HUGE_PQ && queryParams.limitLong() < maxInMemoryResultEntries) {
						throw new UnsupportedOperationException("Allowed limit is " + maxInMemoryResultEntries + " or greater");
					}
					if (sortedByScoreFull != null) {
						return sortedByScoreFull.collectMulti(indexSearchers, queryParams, keyFieldName, transformer);
					} else {
						return scoredPaged.collectMulti(indexSearchers, queryParams, keyFieldName, transformer);
					}
				} else {
					if (!FORCE_HUGE_PQ && queryParams.limitLong() < maxInMemoryResultEntries) {
						throw new UnsupportedOperationException("Allowed limit is " + maxInMemoryResultEntries + " or greater");
					}
					if (sortedScoredFull != null) {
						return sortedScoredFull.collectMulti(indexSearchers, queryParams, keyFieldName, transformer);
					} else {
						return scoredPaged.collectMulti(indexSearchers, queryParams, keyFieldName, transformer);
					}
				}
			}
		} else {
			// Run large/unbounded searches using the continuous multi searcher
			return unsortedUnscoredContinuous.collectMulti(indexSearchers, queryParams, keyFieldName, transformer);
		}
	}

	@Override
	public String getName() {
		return "adaptive local";
	}
}
