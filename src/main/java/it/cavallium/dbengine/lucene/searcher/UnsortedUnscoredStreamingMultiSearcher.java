package it.cavallium.dbengine.lucene.searcher;

import io.net5.buffer.api.Send;
import it.cavallium.dbengine.client.query.current.data.TotalHitsCount;
import it.cavallium.dbengine.database.LLKeyScore;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.disk.LLIndexSearchers;
import it.cavallium.dbengine.lucene.LuceneUtils;
import it.cavallium.dbengine.lucene.collector.ReactiveCollectorManager;
import it.cavallium.dbengine.lucene.searcher.LLSearchTransformer.TransformerInput;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.EmitResult;
import reactor.core.publisher.Sinks.Many;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.concurrent.Queues;

public class UnsortedUnscoredStreamingMultiSearcher implements MultiSearcher {

	private static final Scheduler UNSCORED_UNSORTED_EXECUTOR = Schedulers.newBoundedElastic(
			Schedulers.DEFAULT_BOUNDED_ELASTIC_SIZE,
			Schedulers.DEFAULT_BOUNDED_ELASTIC_QUEUESIZE,
			"UnscoredUnsortedExecutor"
	);
	private static final Supplier<Queue<ScoreDoc>> QUEUE_SUPPLIER = Queues.get(1024);

	@Override
	public Mono<Send<LuceneSearchResult>> collectMulti(Mono<Send<LLIndexSearchers>> indexSearchersMono,
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

			return queryParamsMono
					.flatMap(queryParams2 -> {
						var localQueryParams = getLocalQueryParams(queryParams2);
						if (queryParams2.isSorted() && queryParams2.limitLong() > 0) {
							return Mono.error(new UnsupportedOperationException("Sorted queries are not supported"
									+ " by UnsortedUnscoredContinuousLuceneMultiSearcher"));
						}
						if (queryParams2.needsScores() && queryParams2.limitLong() > 0) {
							return Mono.error(new UnsupportedOperationException("Scored queries are not supported"
									+ " by UnsortedUnscoredContinuousLuceneMultiSearcher"));
						}
						return Mono.fromCallable(() -> {
							LLUtils.ensureBlocking();

							Many<ScoreDoc> scoreDocsSink = Sinks.many().unicast().onBackpressureBuffer(QUEUE_SUPPLIER.get());

							var cm = new ReactiveCollectorManager(scoreDocsSink);

							AtomicInteger runningTasks = new AtomicInteger(0);
							var shards = indexSearchers.shards();

							runningTasks.addAndGet(shards.size());
							int mutableShardIndex = 0;
							for (IndexSearcher shard : shards) {
								int shardIndex = mutableShardIndex++;
								UNSCORED_UNSORTED_EXECUTOR.schedule(() -> {
									try {
										var collector = cm.newCollector();
										assert queryParams.complete() == collector.scoreMode().isExhaustive();
										queryParams.getScoreModeOptional().ifPresent(scoreMode -> {
											assert scoreMode == collector.scoreMode();
										});

										collector.setShardIndex(shardIndex);

										shard.search(localQueryParams.query(), collector);
									} catch (Throwable e) {
										while (scoreDocsSink.tryEmitError(e) == EmitResult.FAIL_NON_SERIALIZED) {
											LockSupport.parkNanos(10);
										}
									} finally {
										if (runningTasks.decrementAndGet() <= 0) {
											while (scoreDocsSink.tryEmitComplete() == EmitResult.FAIL_NON_SERIALIZED) {
												LockSupport.parkNanos(10);
											}
										}
									}
								});
							}

							Flux<LLKeyScore> resultsFlux = LuceneUtils.convertHits(scoreDocsSink.asFlux(), shards, keyFieldName, false);

							var totalHitsCount = new TotalHitsCount(0, false);
							Flux<LLKeyScore> mergedFluxes = resultsFlux
									.skip(queryParams2.offsetLong())
									.take(queryParams2.limitLong(), true);

							return new LuceneSearchResult(totalHitsCount, mergedFluxes, indexSearchers::close).send();
						});
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
				queryParams.complete()
		);
	}

	@Override
	public String getName() {
		return "unsorted unscored streaming multi";
	}
}
