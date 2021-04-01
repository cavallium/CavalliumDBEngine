package it.cavallium.dbengine.lucene.searcher;

import java.io.IOException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.jetbrains.annotations.Nullable;

/**
 * Use a different searcher based on the situation
 */
public class AdaptiveStreamSearcher implements LuceneStreamSearcher {

	private static final boolean ENABLE_PARALLEL_COLLECTOR = true;
	private final SimpleStreamSearcher simpleStreamSearcher;
	private final ParallelCollectorStreamSearcher parallelCollectorStreamSearcher;
	private final PagedStreamSearcher pagedStreamSearcher;
	private final CountStreamSearcher countStreamSearcher;

	public AdaptiveStreamSearcher() {
		this.simpleStreamSearcher = new SimpleStreamSearcher();
		this.countStreamSearcher = new CountStreamSearcher();
		this.parallelCollectorStreamSearcher = new ParallelCollectorStreamSearcher(countStreamSearcher);
		this.pagedStreamSearcher = new PagedStreamSearcher(simpleStreamSearcher);
	}

	@Override
	public LuceneSearchInstance search(IndexSearcher indexSearcher,
			Query query,
			int offset,
			int limit,
			@Nullable Sort luceneSort,
			ScoreMode scoreMode,
			@Nullable Float minCompetitiveScore,
			String keyFieldName) throws IOException {
		if (limit == 0) {
			return countStreamSearcher.count(indexSearcher, query);
		} else if (offset == 0 && luceneSort == null && ENABLE_PARALLEL_COLLECTOR) {
			return parallelCollectorStreamSearcher.search(indexSearcher,
					query,
					offset,
					limit,
					null,
					scoreMode,
					minCompetitiveScore,
					keyFieldName
			);
		} else {
			if (offset > 0 || limit > PagedStreamSearcher.MAX_ITEMS_PER_PAGE) {
				return pagedStreamSearcher.search(indexSearcher,
						query,
						offset,
						limit,
						luceneSort,
						scoreMode,
						minCompetitiveScore,
						keyFieldName
				);
			} else {
				return simpleStreamSearcher.search(indexSearcher,
						query,
						offset,
						limit,
						luceneSort,
						scoreMode,
						minCompetitiveScore,
						keyFieldName
				);
			}
		}
	}
}
