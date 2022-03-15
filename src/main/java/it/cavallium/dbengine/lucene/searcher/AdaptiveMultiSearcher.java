package it.cavallium.dbengine.lucene.searcher;

import static it.cavallium.dbengine.client.UninterruptibleScheduler.uninterruptibleScheduler;
import static it.cavallium.dbengine.lucene.searcher.GlobalQueryRewrite.NO_REWRITE;

import io.net5.buffer.api.Send;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.disk.LLIndexSearchers;
import it.cavallium.dbengine.database.disk.LLTempLMDBEnv;
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

	public AdaptiveMultiSearcher(LLTempLMDBEnv env, boolean useLMDB, int maxInMemoryResultEntries) {
		sortedByScoreFull = useLMDB ? new SortedByScoreFullMultiSearcher(env) : null;
		sortedScoredFull = useLMDB ?  new SortedScoredFullMultiSearcher(env) : null;
		this.maxInMemoryResultEntries = maxInMemoryResultEntries;
	}

	@Override
	public Mono<LuceneSearchResult> collectMulti(Mono<Send<LLIndexSearchers>> indexSearchersMono,
			LocalQueryParams queryParams,
			@Nullable String keyFieldName,
			GlobalQueryRewrite transformer) {
		if (transformer == NO_REWRITE) {
			return transformedCollectMulti(indexSearchersMono, queryParams, keyFieldName, transformer);
		} else {
			return indexSearchersMono
					.publishOn(uninterruptibleScheduler(Schedulers.boundedElastic()))
					.<LocalQueryParams>handle((indexSearchers, sink) -> {
						try {
							sink.next(transformer.rewrite(indexSearchers.receive(), queryParams));
						} catch (IOException ex) {
							sink.error(ex);
						}
					})
					.flatMap(queryParams2 -> transformedCollectMulti(indexSearchersMono, queryParams2, keyFieldName, NO_REWRITE));
		}
	}

	// Remember to change also AdaptiveLocalSearcher
	public Mono<LuceneSearchResult> transformedCollectMulti(Mono<Send<LLIndexSearchers>> indexSearchersMono,
			LocalQueryParams queryParams,
			@Nullable String keyFieldName,
			GlobalQueryRewrite transformer) {
		// offset + limit
		long realLimit = queryParams.offsetLong() + queryParams.limitLong();
		long maxAllowedInMemoryLimit
				= Math.max(maxInMemoryResultEntries, (long) queryParams.pageLimits().getPageLimit(0));

		return LLUtils.usingSendResource(indexSearchersMono, indexSearchers -> {
			if (queryParams.limitLong() == 0) {
				return count.collectMulti(indexSearchersMono, queryParams, keyFieldName, transformer);
			} else if (realLimit <= maxInMemoryResultEntries) {
				return standardSearcher.collectMulti(indexSearchersMono, queryParams, keyFieldName, transformer);
			} else if (queryParams.isSorted()) {
				if (realLimit <= maxAllowedInMemoryLimit) {
					return scoredPaged.collectMulti(indexSearchersMono, queryParams, keyFieldName, transformer);
				} else {
					if (queryParams.isSortedByScore()) {
						if (queryParams.limitLong() < maxInMemoryResultEntries) {
							throw new UnsupportedOperationException("Allowed limit is " + maxInMemoryResultEntries + " or greater");
						}
						if (sortedByScoreFull != null) {
							return sortedByScoreFull.collectMulti(indexSearchersMono, queryParams, keyFieldName, transformer);
						} else {
							return scoredPaged.collectMulti(indexSearchersMono, queryParams, keyFieldName, transformer);
						}
					} else {
						if (queryParams.limitLong() < maxInMemoryResultEntries) {
							throw new UnsupportedOperationException("Allowed limit is " + maxInMemoryResultEntries + " or greater");
						}
						if (sortedScoredFull != null) {
							return sortedScoredFull.collectMulti(indexSearchersMono, queryParams, keyFieldName, transformer);
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
