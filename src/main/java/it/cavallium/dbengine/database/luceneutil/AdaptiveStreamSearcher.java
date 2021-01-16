package it.cavallium.dbengine.database.luceneutil;

import java.io.IOException;
import java.util.function.Consumer;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.jetbrains.annotations.Nullable;

/**
 * Use a different searcher based on the situation
 */
public class AdaptiveStreamSearcher implements LuceneStreamSearcher {

	private final SimpleStreamSearcher simpleStreamSearcher;
	private final ParallelCollectorStreamSearcher parallelCollectorStreamSearcher;
	private final PagedStreamSearcher pagedStreamSearcher;
	private final CountStreamSearcher countStreamSearcher;

	public AdaptiveStreamSearcher() {
		this.simpleStreamSearcher = new SimpleStreamSearcher();
		this.parallelCollectorStreamSearcher = new ParallelCollectorStreamSearcher();
		this.pagedStreamSearcher = new PagedStreamSearcher(simpleStreamSearcher);
		this.countStreamSearcher = new CountStreamSearcher();
	}

	@Override
	public Long streamSearch(IndexSearcher indexSearcher,
			Query query,
			int limit,
			@Nullable Sort luceneSort,
			String keyFieldName,
			Consumer<String> consumer) throws IOException {
		if (limit == 0) {
			return countStreamSearcher.count(indexSearcher, query);
		} else if (luceneSort == null) {
			return parallelCollectorStreamSearcher.streamSearch(indexSearcher, query, limit, null, keyFieldName, consumer);
		} else {
			if (limit > PagedStreamSearcher.MAX_ITEMS_PER_PAGE) {
				return pagedStreamSearcher.streamSearch(indexSearcher, query, limit, luceneSort, keyFieldName, consumer);
			} else {
				return simpleStreamSearcher.streamSearch(indexSearcher, query, limit, luceneSort, keyFieldName, consumer);
			}
		}
	}
}
