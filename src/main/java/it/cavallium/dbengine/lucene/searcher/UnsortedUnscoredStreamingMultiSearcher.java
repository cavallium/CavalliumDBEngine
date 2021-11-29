package it.cavallium.dbengine.lucene.searcher;

import io.net5.buffer.api.Send;
import it.cavallium.dbengine.client.query.current.data.TotalHitsCount;
import it.cavallium.dbengine.database.LLKeyScore;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.disk.LLIndexSearchers;
import it.cavallium.dbengine.lucene.LuceneUtils;
import it.cavallium.dbengine.lucene.collector.ReactiveCollectorMultiManager;
import it.cavallium.dbengine.lucene.searcher.LLSearchTransformer.TransformerInput;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.warp.commonutils.type.ShortNamedThreadFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink.OverflowStrategy;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class UnsortedUnscoredStreamingMultiSearcher implements MultiSearcher {

  private static final Scheduler SCHEDULER = Schedulers.fromExecutorService(Executors.newCachedThreadPool(
			new ShortNamedThreadFactory("UnscoredStreamingSearcher")), "UnscoredStreamingSearcher");

	@Override
	public Mono<LuceneSearchResult> collectMulti(Mono<Send<LLIndexSearchers>> indexSearchersMono,
			LocalQueryParams queryParams,
			String keyFieldName,
			LLSearchTransformer transformer) {

		return LLUtils.usingSendResource(indexSearchersMono, indexSearchers -> {
			Mono<LocalQueryParams> queryParamsMono;
			if (transformer == LLSearchTransformer.NO_TRANSFORMATION) {
				queryParamsMono = Mono.just(queryParams);
			} else {
				queryParamsMono = transformer.transform(Mono
						.fromCallable(() -> new TransformerInput(indexSearchers, queryParams)));
			}

			return queryParamsMono.map(queryParams2 -> {
				var localQueryParams = getLocalQueryParams(queryParams2);
				if (queryParams2.isSorted() && queryParams2.limitLong() > 0) {
					throw new UnsupportedOperationException("Sorted queries are not supported"
							+ " by UnsortedUnscoredContinuousLuceneMultiSearcher");
				}
				if (queryParams2.needsScores() && queryParams2.limitLong() > 0) {
					throw new UnsupportedOperationException("Scored queries are not supported"
							+ " by UnsortedUnscoredContinuousLuceneMultiSearcher");
				}
				var shards = indexSearchers.shards();

				Flux<ScoreDoc> scoreDocsFlux = Flux.<ScoreDoc>create(scoreDocsSink -> {
					LLUtils.ensureBlocking();
					var currentThread = Thread.currentThread();
					var cmm = new ReactiveCollectorMultiManager(scoreDocsSink, currentThread);

					//// Unpark the paused request thread
					scoreDocsSink.onRequest(n -> LockSupport.unpark(currentThread));

					int mutableShardIndex = 0;
					for (IndexSearcher shard : shards) {
						int shardIndex = mutableShardIndex++;
						try {
							var collectorManager = cmm.get(shardIndex);
							assert queryParams.computePreciseHitsCount() == cmm.scoreMode().isExhaustive();

							var executor = shard.getExecutor();
							if (executor == null) {
								//noinspection BlockingMethodInNonBlockingContext
								shard.search(localQueryParams.query(), collectorManager);
							} else {
								// Avoid using the index searcher executor to avoid blocking on its threads
								//noinspection BlockingMethodInNonBlockingContext
								shard.search(localQueryParams.query(), collectorManager.newCollector());
							}
						} catch (Throwable e) {
							scoreDocsSink.error(e);
						} finally {
							scoreDocsSink.complete();
						}
					}
				}, OverflowStrategy.ERROR)
						.limitRate(2048, 256)
						.subscribeOn(SCHEDULER, true);


				Flux<LLKeyScore> resultsFlux = LuceneUtils.convertHits(scoreDocsFlux, shards, keyFieldName, false);

				var totalHitsCount = new TotalHitsCount(0, false);
				Flux<LLKeyScore> mergedFluxes = resultsFlux
						.skip(queryParams2.offsetLong())
						.take(queryParams2.limitLong(), true);

				return new LuceneSearchResult(totalHitsCount, mergedFluxes, indexSearchers::close);
			});
		}, false);
	}

	private LocalQueryParams getLocalQueryParams(LocalQueryParams queryParams) {
		return new LocalQueryParams(queryParams.query(),
				0L,
				queryParams.offsetLong() + queryParams.limitLong(),
				queryParams.pageLimits(),
				queryParams.minCompetitiveScore(),
				queryParams.sort(),
				queryParams.computePreciseHitsCount()
		);
	}

	@Override
	public String getName() {
		return "unsorted unscored streaming multi";
	}
}
