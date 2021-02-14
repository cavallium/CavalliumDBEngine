package it.cavallium.dbengine.lucene.searcher;

import it.cavallium.dbengine.database.LLKeyScore;
import java.io.IOException;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
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
		this.parallelCollectorStreamSearcher = new ParallelCollectorStreamSearcher();
		this.pagedStreamSearcher = new PagedStreamSearcher(simpleStreamSearcher);
		this.countStreamSearcher = new CountStreamSearcher();
	}

	@Override
	public void search(IndexSearcher indexSearcher,
			Query query,
			int limit,
			@Nullable Sort luceneSort,
			ScoreMode scoreMode,
			@Nullable Float minCompetitiveScore,
			String keyFieldName,
			Consumer<LLKeyScore> consumer,
			LongConsumer totalHitsConsumer) throws IOException {
		if (limit == 0) {
			totalHitsConsumer.accept(countStreamSearcher.count(indexSearcher, query));
		} else if (luceneSort == null && ENABLE_PARALLEL_COLLECTOR) {
			parallelCollectorStreamSearcher.search(indexSearcher,
					query,
					limit,
					null,
					scoreMode,
					minCompetitiveScore,
					keyFieldName,
					consumer,
					totalHitsConsumer
			);
		} else {
			if (luceneSort != null && limit > PagedStreamSearcher.MAX_ITEMS_PER_PAGE) {
				pagedStreamSearcher.search(indexSearcher,
						query,
						limit,
						luceneSort,
						scoreMode,
						minCompetitiveScore,
						keyFieldName,
						consumer,
						totalHitsConsumer
				);
			} else {
				simpleStreamSearcher.search(indexSearcher,
						query,
						limit,
						luceneSort,
						scoreMode,
						minCompetitiveScore,
						keyFieldName,
						consumer,
						totalHitsConsumer
				);
			}
		}
	}
}
