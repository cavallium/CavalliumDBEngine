package it.cavallium.dbengine.lucene.searcher;

import static it.cavallium.dbengine.client.UninterruptibleScheduler.uninterruptibleScheduler;
import static it.cavallium.dbengine.database.LLUtils.singleOrClose;
import static it.cavallium.dbengine.lucene.searcher.GlobalQueryRewrite.NO_REWRITE;

import io.netty5.buffer.api.Send;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.disk.LLIndexSearchers;
import it.cavallium.dbengine.database.disk.LLTempHugePqEnv;
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
		sortedByScoreFull = useHugePq ? new SortedByScoreFullMultiSearcher(env) : null;
		sortedScoredFull = useHugePq ?  new SortedScoredFullMultiSearcher(env) : null;
		this.maxInMemoryResultEntries = maxInMemoryResultEntries;
	}

	@Override
	public Mono<LuceneSearchResult> collectMulti(Mono<LLIndexSearchers> indexSearchersMono,
			LocalQueryParams queryParams,
			@Nullable String keyFieldName,
			GlobalQueryRewrite transformer) {
		return singleOrClose(indexSearchersMono, indexSearchers -> {
			if (transformer == NO_REWRITE) {
				return transformedCollectMulti(indexSearchers, queryParams, keyFieldName, transformer);
			} else {
				return Mono.fromCallable(() -> transformer.rewrite(indexSearchers, queryParams))
						.subscribeOn(uninterruptibleScheduler(Schedulers.boundedElastic()))
						.flatMap(queryParams2 -> transformedCollectMulti(indexSearchers, queryParams2, keyFieldName, NO_REWRITE));
			}
		});
	}

	// Remember to change also AdaptiveLocalSearcher
	public Mono<LuceneSearchResult> transformedCollectMulti(LLIndexSearchers indexSearchers,
			LocalQueryParams queryParams,
			@Nullable String keyFieldName,
			GlobalQueryRewrite transformer) {
		// offset + limit
		long realLimit = queryParams.offsetLong() + queryParams.limitLong();
		long maxAllowedInMemoryLimit
				= Math.max(maxInMemoryResultEntries, (long) queryParams.pageLimits().getPageLimit(0));

		if (queryParams.limitLong() == 0) {
			return count.collectMulti(Mono.just(indexSearchers), queryParams, keyFieldName, transformer);
		} else if (realLimit <= maxInMemoryResultEntries) {
			return standardSearcher.collectMulti(Mono.just(indexSearchers), queryParams, keyFieldName, transformer);
		} else if (queryParams.isSorted()) {
			if (realLimit <= maxAllowedInMemoryLimit) {
				return scoredPaged.collectMulti(Mono.just(indexSearchers), queryParams, keyFieldName, transformer);
			} else {
				if (queryParams.isSortedByScore()) {
					if (queryParams.limitLong() < maxInMemoryResultEntries) {
						throw new UnsupportedOperationException("Allowed limit is " + maxInMemoryResultEntries + " or greater");
					}
					if (sortedByScoreFull != null) {
						return sortedByScoreFull.collectMulti(Mono.just(indexSearchers), queryParams, keyFieldName, transformer);
					} else {
						return scoredPaged.collectMulti(Mono.just(indexSearchers), queryParams, keyFieldName, transformer);
					}
				} else {
					if (queryParams.limitLong() < maxInMemoryResultEntries) {
						throw new UnsupportedOperationException("Allowed limit is " + maxInMemoryResultEntries + " or greater");
					}
					if (sortedScoredFull != null) {
						return sortedScoredFull.collectMulti(Mono.just(indexSearchers), queryParams, keyFieldName, transformer);
					} else {
						return scoredPaged.collectMulti(Mono.just(indexSearchers), queryParams, keyFieldName, transformer);
					}
				}
			}
		} else {
			// Run large/unbounded searches using the continuous multi searcher
			return unsortedUnscoredContinuous.collectMulti(Mono.just(indexSearchers), queryParams, keyFieldName, transformer);
		}
	}

	@Override
	public String getName() {
		return "adaptive local";
	}
}
