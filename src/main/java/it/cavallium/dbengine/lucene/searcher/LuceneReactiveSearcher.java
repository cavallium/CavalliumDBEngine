package it.cavallium.dbengine.lucene.searcher;

import java.io.IOException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.jetbrains.annotations.Nullable;
import org.warp.commonutils.log.Logger;
import org.warp.commonutils.log.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

public interface LuceneReactiveSearcher {

	Logger logger = LoggerFactory.getLogger(LuceneReactiveSearcher.class);

	/**
	 * Do a lucene query, receiving the single results using a consumer
	 * @param indexSearcher the index searcher, which contains all the lucene data
	 * @param query the query
	 * @param offset the offset of the first result (use 0 to disable offset)
	 * @param limit the maximum number of results
	 * @param luceneSort the sorting method used for the search
	 * @param scoreMode score mode
	 * @param minCompetitiveScore minimum score accepted
	 * @param keyFieldName the name of the key field
	 * @param scheduler a blocking scheduler
	 */
	Mono<LuceneReactiveSearchInstance> search(IndexSearcher indexSearcher,
			Query query,
			int offset,
			int limit,
			@Nullable Sort luceneSort,
			ScoreMode scoreMode,
			@Nullable Float minCompetitiveScore,
			String keyFieldName,
			Scheduler scheduler);
}
