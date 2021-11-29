package it.cavallium.dbengine.lucene.searcher;

import io.net5.buffer.api.Send;
import it.cavallium.dbengine.client.query.current.data.TotalHitsCount;
import it.cavallium.dbengine.database.LLKeyScore;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.disk.LLIndexSearchers;
import it.cavallium.dbengine.lucene.LuceneUtils;
import it.cavallium.dbengine.lucene.collector.ReactiveCollectorMultiManager;
import it.cavallium.dbengine.lucene.searcher.LLSearchTransformer.TransformerInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import org.apache.commons.lang3.concurrent.TimedSemaphore;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.warp.commonutils.type.ShortNamedThreadFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink.OverflowStrategy;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class UnsortedUnscoredStreamingMultiSearcher implements MultiSearcher {

  private static final ExecutorService SCHEDULER = Executors.newCachedThreadPool(new ShortNamedThreadFactory(
			"UnscoredStreamingSearcher"));

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
					var requested = new LongSemaphore(0);
					var cmm = new ReactiveCollectorMultiManager(scoreDocsSink, requested);

					scoreDocsSink.onRequest(requested::release);

					int mutableShardIndex = 0;
					CompletableFuture<?>[] futures = new CompletableFuture<?>[shards.size()];
					for (IndexSearcher shard : shards) {
						int shardIndex = mutableShardIndex++;
						assert queryParams.computePreciseHitsCount() == cmm.scoreMode().isExhaustive();

						var future = CompletableFuture.runAsync(() -> {
							try {
								LLUtils.ensureBlocking();
								var collectorManager = cmm.get(shardIndex);
								shard.search(localQueryParams.query(), collectorManager);
							} catch (IOException e) {
								throw new CompletionException(e);
							}
						}, SCHEDULER);

						futures[shardIndex] = future;
					}
					var combinedFuture = CompletableFuture.allOf(futures).whenCompleteAsync((result, ex) -> {
						if (ex != null) {
							scoreDocsSink.error(ex);
						} else {
							scoreDocsSink.complete();
						}
					});
					scoreDocsSink.onCancel(() -> {
						for (CompletableFuture<?> future : futures) {
							future.cancel(true);
						}
						combinedFuture.cancel(true);
					});
				}, OverflowStrategy.ERROR);


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
