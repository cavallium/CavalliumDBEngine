package it.cavallium.dbengine.lucene.searcher;

import static it.cavallium.dbengine.client.UninterruptibleScheduler.uninterruptibleScheduler;
import static it.cavallium.dbengine.database.LLUtils.singleOrClose;
import static it.cavallium.dbengine.lucene.searcher.GlobalQueryRewrite.NO_REWRITE;

import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.disk.LLIndexSearcher;
import it.cavallium.dbengine.database.disk.LLIndexSearchers;
import it.cavallium.dbengine.database.disk.LLTempHugePqEnv;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class AdaptiveLocalSearcher implements LocalSearcher {

	static final boolean FORCE_HUGE_PQ
			= Boolean.parseBoolean(System.getProperty("it.cavallium.hugepq.force", "false"));

	private static final StandardSearcher standardSearcher = new StandardSearcher();

	private static final LocalSearcher scoredPaged = new PagedLocalSearcher();

	private static final LocalSearcher countSearcher = new CountMultiSearcher();

	private static final MultiSearcher unsortedUnscoredContinuous = new UnsortedStreamingMultiSearcher();

	/**
	 * Use in-memory collectors if the expected results count is lower or equal than this limit
	 */
	private final int maxInMemoryResultEntries;

	@Nullable
	private final SortedByScoreFullMultiSearcher sortedByScoreFull;

	@Nullable
	private final SortedScoredFullMultiSearcher sortedScoredFull;

	public AdaptiveLocalSearcher(LLTempHugePqEnv env, boolean useHugePq, int maxInMemoryResultEntries) {
		sortedByScoreFull = (FORCE_HUGE_PQ || useHugePq) ? new SortedByScoreFullMultiSearcher(env) : null;
		sortedScoredFull = (FORCE_HUGE_PQ || useHugePq) ? new SortedScoredFullMultiSearcher(env) : null;
		this.maxInMemoryResultEntries = maxInMemoryResultEntries;
	}

	@Override
	public Mono<LuceneSearchResult> collect(Mono<LLIndexSearcher> indexSearcherMono,
			LocalQueryParams queryParams,
			@Nullable String keyFieldName,
			GlobalQueryRewrite transformer) {
		return singleOrClose(indexSearcherMono, indexSearcher -> {
			var indexSearchers = LLIndexSearchers.unsharded(indexSearcher);

			if (transformer == NO_REWRITE) {
				return transformedCollect(indexSearcher, queryParams, keyFieldName, transformer);
			} else {
				return Mono
						.fromCallable(() -> transformer.rewrite(indexSearchers, queryParams))
						.subscribeOn(uninterruptibleScheduler(Schedulers.boundedElastic()))
						.flatMap(queryParams2 -> transformedCollect(indexSearcher, queryParams2, keyFieldName, NO_REWRITE));
			}
		});
	}

	@Override
	public String getName() {
		return "adaptivelocal";
	}

	// Remember to change also AdaptiveMultiSearcher
	public Mono<LuceneSearchResult> transformedCollect(LLIndexSearcher indexSearcher,
			LocalQueryParams queryParams,
			String keyFieldName,
			GlobalQueryRewrite transformer) {
		// offset + limit
		long realLimit = queryParams.offsetLong() + queryParams.limitLong();
		long maxAllowedInMemoryLimit
				= Math.max(maxInMemoryResultEntries, (long) queryParams.pageLimits().getPageLimit(0));

		if (!FORCE_HUGE_PQ && queryParams.limitLong() == 0) {
			return countSearcher.collect(Mono.just(indexSearcher), queryParams, keyFieldName, transformer);
		} else if (!FORCE_HUGE_PQ && realLimit <= maxInMemoryResultEntries) {
			return standardSearcher.collect(Mono.just(indexSearcher), queryParams, keyFieldName, transformer);
		} else if (FORCE_HUGE_PQ || queryParams.isSorted()) {
			if (!FORCE_HUGE_PQ && realLimit <= maxAllowedInMemoryLimit) {
				return scoredPaged.collect(Mono.just(indexSearcher), queryParams, keyFieldName, transformer);
			} else {
				if (queryParams.isSortedByScore()) {
					if (!FORCE_HUGE_PQ && queryParams.limitLong() < maxInMemoryResultEntries) {
						throw new UnsupportedOperationException("Allowed limit is " + maxInMemoryResultEntries + " or greater");
					}
					if (sortedByScoreFull != null) {
						return sortedByScoreFull.collect(Mono.just(indexSearcher), queryParams, keyFieldName, transformer);
					} else {
						return scoredPaged.collect(Mono.just(indexSearcher), queryParams, keyFieldName, transformer);
					}
				} else {
					if (!FORCE_HUGE_PQ && queryParams.limitLong() < maxInMemoryResultEntries) {
						throw new UnsupportedOperationException("Allowed limit is " + maxInMemoryResultEntries + " or greater");
					}
					if (sortedScoredFull != null) {
						return sortedScoredFull.collect(Mono.just(indexSearcher), queryParams, keyFieldName, transformer);
					} else {
						return scoredPaged.collect(Mono.just(indexSearcher), queryParams, keyFieldName, transformer);
					}
				}
			}
		} else {
			// Run large/unbounded searches using the continuous multi searcher
			return unsortedUnscoredContinuous.collect(Mono.just(indexSearcher), queryParams, keyFieldName, transformer);
		}
	}
}
