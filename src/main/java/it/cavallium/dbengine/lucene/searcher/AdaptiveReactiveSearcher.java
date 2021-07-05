package it.cavallium.dbengine.lucene.searcher;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

public class AdaptiveReactiveSearcher implements LuceneReactiveSearcher {

	public static final int PAGED_THRESHOLD = 1000;
	private static final LuceneReactiveSearcher count = new CountLuceneReactiveSearcher();

	private static final LuceneReactiveSearcher paged = new PagedLuceneReactiveSearcher();
	private static final LuceneReactiveSearcher simple = new SimpleLuceneReactiveSearcher();

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
		if (offset + limit > PAGED_THRESHOLD) {
			return paged.search(indexSearcher,
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
			return simple.search(indexSearcher,
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
