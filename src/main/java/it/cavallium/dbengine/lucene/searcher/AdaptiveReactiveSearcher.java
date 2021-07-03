package it.cavallium.dbengine.lucene.searcher;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

public class AdaptiveReactiveSearcher implements LuceneReactiveSearcher {

	private static final LuceneReactiveSearcher count = new CountLuceneReactiveSearcher();

	private static final LuceneReactiveSearcher sortedPaged = new SortedPagedLuceneReactiveSearcher();
	private static final LuceneReactiveSearcher unsortedPaged = new UnsortedPagedLuceneReactiveSearcher();

	@Override
	public Mono<LuceneReactiveSearchInstance> search(IndexSearcher indexSearcher,
			Query query,
			int offset,
			int limit,
			@Nullable Sort luceneSort,
			ScoreMode scoreMode,
			@Nullable Float minCompetitiveScore,
			String keyFieldName,
			Scheduler scheduler) {
		if (limit == 0) {
			return count.search(indexSearcher,
					query,
					offset,
					limit,
					luceneSort,
					scoreMode,
					minCompetitiveScore,
					keyFieldName,
					scheduler
			);
		}
		if (luceneSort != null) {
			return sortedPaged.search(indexSearcher,
					query,
					offset,
					limit,
					luceneSort,
					scoreMode,
					minCompetitiveScore,
					keyFieldName,
					scheduler
			);
		} else {
			return unsortedPaged.search(indexSearcher,
					query,
					offset,
					limit,
					luceneSort,
					scoreMode,
					minCompetitiveScore,
					keyFieldName,
					scheduler
			);
		}
	}
}
