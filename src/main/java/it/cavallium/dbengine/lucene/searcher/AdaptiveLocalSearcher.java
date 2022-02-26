package it.cavallium.dbengine.lucene.searcher;

import static it.cavallium.dbengine.client.UninterruptibleScheduler.uninterruptibleScheduler;
import static it.cavallium.dbengine.lucene.searcher.GlobalQueryRewrite.NO_REWRITE;

import io.net5.buffer.api.Send;
import io.net5.buffer.api.internal.ResourceSupport;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.disk.LLIndexSearcher;
import it.cavallium.dbengine.database.disk.LLIndexSearchers;
import it.cavallium.dbengine.database.disk.LLTempLMDBEnv;
import java.io.IOException;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class AdaptiveLocalSearcher implements LocalSearcher {

	private static final OfficialSearcher officialSearcher = new OfficialSearcher();

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

	public AdaptiveLocalSearcher(LLTempLMDBEnv env, boolean useLMDB, int maxInMemoryResultEntries) {
		sortedByScoreFull = useLMDB ? new SortedByScoreFullMultiSearcher(env) : null;
		sortedScoredFull = useLMDB ? new SortedScoredFullMultiSearcher(env) : null;
		this.maxInMemoryResultEntries = maxInMemoryResultEntries;
	}

	@Override
	public Mono<LuceneSearchResult> collect(Mono<Send<LLIndexSearcher>> indexSearcher,
			LocalQueryParams queryParams,
			@Nullable String keyFieldName,
			GlobalQueryRewrite transformer) {
		Mono<Send<LLIndexSearchers>> indexSearchersMono = indexSearcher
				.map(LLIndexSearchers::unsharded)
				.map(ResourceSupport::send);

		if (transformer == NO_REWRITE) {
			return transformedCollect(indexSearcher, queryParams, keyFieldName, transformer);
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
					.flatMap(queryParams2 -> transformedCollect(indexSearcher, queryParams2, keyFieldName, NO_REWRITE));
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
			GlobalQueryRewrite transformer) {
		// offset + limit
		long realLimit = queryParams.offsetLong() + queryParams.limitLong();
		long maxAllowedInMemoryLimit
				= Math.max(maxInMemoryResultEntries, (long) queryParams.pageLimits().getPageLimit(0));

		if (queryParams.limitLong() == 0) {
			return countSearcher.collect(indexSearcher, queryParams, keyFieldName, transformer);
		} else if (queryParams.isSorted()) {
			if (realLimit <= maxAllowedInMemoryLimit) {
				return scoredPaged.collect(indexSearcher, queryParams, keyFieldName, transformer);
			} else {
				if (queryParams.isSortedByScore()) {
					if (queryParams.limitLong() < maxInMemoryResultEntries) {
						throw new UnsupportedOperationException("Allowed limit is " + maxInMemoryResultEntries + " or greater");
					}
					if (sortedByScoreFull != null) {
						return sortedByScoreFull.collect(indexSearcher, queryParams, keyFieldName, transformer);
					} else {
						return scoredPaged.collect(indexSearcher, queryParams, keyFieldName, transformer);
					}
				} else {
					if (queryParams.limitLong() < maxInMemoryResultEntries) {
						throw new UnsupportedOperationException("Allowed limit is " + maxInMemoryResultEntries + " or greater");
					}
					if (sortedScoredFull != null) {
						return sortedScoredFull.collect(indexSearcher, queryParams, keyFieldName, transformer);
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
