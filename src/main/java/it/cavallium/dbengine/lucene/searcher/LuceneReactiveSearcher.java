package it.cavallium.dbengine.lucene.searcher;

import it.cavallium.dbengine.database.LLKeyScore;
import it.cavallium.dbengine.lucene.LuceneUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.jetbrains.annotations.Nullable;
import org.warp.commonutils.log.Logger;
import org.warp.commonutils.log.LoggerFactory;
import reactor.core.publisher.Flux;
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

	static Flux<LLKeyScore> convertHits(
			ScoreDoc[] hits,
			IndexSearcher indexSearcher,
			@Nullable Float minCompetitiveScore,
			String keyFieldName,
			Scheduler scheduler) {
		return Flux
				.fromArray(hits)
				.map(hit -> {
					int shardDocId = hit.doc;
					float score = hit.score;
					var keyMono = Mono.fromCallable(() -> {
						if (!LuceneUtils.filterTopDoc(score, minCompetitiveScore)) {
							return null;
						}
						//noinspection BlockingMethodInNonBlockingContext
						@Nullable String collectedDoc = LuceneUtils.keyOfTopDoc(logger, shardDocId, indexSearcher, keyFieldName);
						return collectedDoc;
					}).subscribeOn(scheduler);
					return new LLKeyScore(shardDocId, score, keyMono);
				});
	}
}
