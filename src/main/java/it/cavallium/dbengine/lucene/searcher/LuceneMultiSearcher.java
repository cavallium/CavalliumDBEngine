package it.cavallium.dbengine.lucene.searcher;

import it.cavallium.dbengine.client.query.current.data.QueryParams;
import it.cavallium.dbengine.database.LLKeyScore;
import it.cavallium.dbengine.lucene.LuceneUtils;
import java.util.ArrayList;
import java.util.List;
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

public interface LuceneMultiSearcher {

	Logger logger = LoggerFactory.getLogger(LuceneMultiSearcher.class);

	/**
	 * Do a lucene query, receiving the single results using a consumer
	 * @param queryParams the query parameters
	 */
	Mono<LuceneShardSearcher> createShardSearcher(QueryParams queryParams);

	static Flux<LLKeyScore> convertHits(
			ScoreDoc[] hits,
			IndexSearchers indexSearchers,
			String keyFieldName,
			Scheduler scheduler) {
		return Flux
				.fromArray(hits)
				.map(hit -> {
					int shardDocId = hit.doc;
					int shardIndex = hit.shardIndex;
					float score = hit.score;
					var indexSearcher = indexSearchers.shard(shardIndex);
					var keyMono = Mono.fromCallable(() -> {
						//noinspection BlockingMethodInNonBlockingContext
						@Nullable String collectedDoc = LuceneUtils.keyOfTopDoc(logger, shardDocId, indexSearcher.getIndexReader(), keyFieldName);
						return collectedDoc;
					}).subscribeOn(scheduler);
					return new LLKeyScore(shardDocId, score, keyMono);
				});
	}
}
