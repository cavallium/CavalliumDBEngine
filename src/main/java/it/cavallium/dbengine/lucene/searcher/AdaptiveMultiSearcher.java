package it.cavallium.dbengine.lucene.searcher;

import static it.cavallium.dbengine.lucene.searcher.GlobalQueryRewrite.NO_REWRITE;

import it.cavallium.dbengine.database.disk.LLIndexSearchers;
import it.cavallium.dbengine.lucene.LuceneUtils;
import java.io.IOException;
import org.jetbrains.annotations.Nullable;

public class AdaptiveMultiSearcher implements MultiSearcher {

	private static final StandardSearcher standardSearcher = new StandardSearcher();

	private static final MultiSearcher count = new CountMultiSearcher();

	private static final MultiSearcher scoredPaged = new ScoredPagedMultiSearcher();

	private static final MultiSearcher unsortedUnscoredContinuous = new UnsortedStreamingMultiSearcher();

	/**
	 * Use in-memory collectors if the expected results count is lower or equal than this limit
	 */
	private final int maxInMemoryResultEntries;

	public AdaptiveMultiSearcher(int maxInMemoryResultEntries) {
		this.maxInMemoryResultEntries = maxInMemoryResultEntries;
	}

	@Override
	public LuceneSearchResult collectMulti(LLIndexSearchers indexSearchers,
			LocalQueryParams queryParams,
			@Nullable String keyFieldName,
			GlobalQueryRewrite transformer) {
		if (transformer != NO_REWRITE) {
			return LuceneUtils.rewriteMulti(this, indexSearchers, queryParams, keyFieldName, transformer);
		}
		return transformedCollectMulti(indexSearchers, queryParams, keyFieldName, transformer);
	}

	// Remember to change also AdaptiveLocalSearcher
	public LuceneSearchResult transformedCollectMulti(LLIndexSearchers indexSearchers,
			LocalQueryParams queryParams,
			@Nullable String keyFieldName,
			GlobalQueryRewrite transformer) {
		// offset + limit
		long realLimit = queryParams.offsetLong() + queryParams.limitLong();
		long maxAllowedInMemoryLimit
				= Math.max(maxInMemoryResultEntries, (long) queryParams.pageLimits().getPageLimit(0));

		if (queryParams.limitLong() == 0) {
			return count.collectMulti(indexSearchers, queryParams, keyFieldName, transformer);
		} else if (realLimit <= maxInMemoryResultEntries) {
			return standardSearcher.collectMulti(indexSearchers, queryParams, keyFieldName, transformer);
		} else if (queryParams.isSorted()) {
			if (realLimit <= maxAllowedInMemoryLimit) {
				return scoredPaged.collectMulti(indexSearchers, queryParams, keyFieldName, transformer);
			} else {
				if (queryParams.isSortedByScore()) {
					if (queryParams.limitLong() < maxInMemoryResultEntries) {
						throw new UnsupportedOperationException("Allowed limit is " + maxInMemoryResultEntries + " or greater");
					}
					return scoredPaged.collectMulti(indexSearchers, queryParams, keyFieldName, transformer);
				} else {
					if (queryParams.limitLong() < maxInMemoryResultEntries) {
						throw new UnsupportedOperationException("Allowed limit is " + maxInMemoryResultEntries + " or greater");
					}
					return scoredPaged.collectMulti(indexSearchers, queryParams, keyFieldName, transformer);
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
