package it.cavallium.dbengine.lucene.searcher;

import static it.cavallium.dbengine.lucene.searcher.GlobalQueryRewrite.NO_REWRITE;

import it.cavallium.dbengine.database.disk.LLIndexSearcher;
import it.cavallium.dbengine.lucene.LuceneUtils;
import java.io.IOException;
import org.jetbrains.annotations.Nullable;

public class AdaptiveLocalSearcher implements LocalSearcher {

	private static final StandardSearcher standardSearcher = new StandardSearcher();

	private static final LocalSearcher scoredPaged = new PagedLocalSearcher();

	private static final LocalSearcher countSearcher = new CountMultiSearcher();

	private static final MultiSearcher unsortedUnscoredContinuous = new UnsortedStreamingMultiSearcher();

	/**
	 * Use in-memory collectors if the expected results count is lower or equal than this limit
	 */
	private final int maxInMemoryResultEntries;

	public AdaptiveLocalSearcher(int maxInMemoryResultEntries) {
		this.maxInMemoryResultEntries = maxInMemoryResultEntries;
	}

	@Override
	public LuceneSearchResult collect(LLIndexSearcher indexSearcher,
			LocalQueryParams queryParams,
			@Nullable String keyFieldName,
			GlobalQueryRewrite transformer) {
		if (transformer != NO_REWRITE) {
			try {
				return LuceneUtils.rewrite(this, indexSearcher, queryParams, keyFieldName, transformer);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
		return transformedCollect(indexSearcher, queryParams, keyFieldName, transformer);
	}

	@Override
	public String getName() {
		return "adaptivelocal";
	}

	// Remember to change also AdaptiveMultiSearcher
	public LuceneSearchResult transformedCollect(LLIndexSearcher indexSearcher,
			LocalQueryParams queryParams,
			String keyFieldName,
			GlobalQueryRewrite transformer) {
		// offset + limit
		long realLimit = queryParams.offsetLong() + queryParams.limitLong();
		long maxAllowedInMemoryLimit
				= Math.max(maxInMemoryResultEntries, (long) queryParams.pageLimits().getPageLimit(0));

		if (queryParams.limitLong() == 0) {
			return countSearcher.collect(indexSearcher, queryParams, keyFieldName, transformer);
		} else if (realLimit <= maxInMemoryResultEntries) {
			return standardSearcher.collect(indexSearcher, queryParams, keyFieldName, transformer);
		} else if (queryParams.isSorted()) {
			if (realLimit <= maxAllowedInMemoryLimit) {
				return scoredPaged.collect(indexSearcher, queryParams, keyFieldName, transformer);
			} else {
				if (queryParams.isSortedByScore()) {
					if (queryParams.limitLong() < maxInMemoryResultEntries) {
						throw new UnsupportedOperationException("Allowed limit is " + maxInMemoryResultEntries + " or greater");
					}
					return scoredPaged.collect(indexSearcher, queryParams, keyFieldName, transformer);
				} else {
					if (queryParams.limitLong() < maxInMemoryResultEntries) {
						throw new UnsupportedOperationException("Allowed limit is " + maxInMemoryResultEntries + " or greater");
					}
					return scoredPaged.collect(indexSearcher, queryParams, keyFieldName, transformer);
				}
			}
		} else {
			// Run large/unbounded searches using the continuous multi searcher
			return unsortedUnscoredContinuous.collect(indexSearcher, queryParams, keyFieldName, transformer);
		}
	}
}
