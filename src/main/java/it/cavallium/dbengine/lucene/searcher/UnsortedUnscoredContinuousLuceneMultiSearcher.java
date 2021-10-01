package it.cavallium.dbengine.lucene.searcher;

import io.net5.buffer.api.Send;
import it.cavallium.dbengine.client.query.current.data.TotalHitsCount;
import it.cavallium.dbengine.database.LLKeyScore;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.disk.LLIndexSearchers;
import it.cavallium.dbengine.lucene.LuceneUtils;
import it.cavallium.dbengine.lucene.collector.ReactiveCollectorManager;
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

public class UnsortedUnscoredContinuousLuceneMultiSearcher implements LuceneMultiSearcher {

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
		var indexSearchersSendResource = Mono
				.fromRunnable(() -> {
					LLUtils.ensureBlocking();
					if (queryParams.isSorted() && queryParams.limit() > 0) {
						throw new UnsupportedOperationException("Sorted queries are not supported"
								+ " by UnsortedUnscoredContinuousLuceneMultiSearcher");
					}
					if (queryParams.isScored() && queryParams.limit() > 0) {
						throw new UnsupportedOperationException("Scored queries are not supported"
								+ " by UnsortedUnscoredContinuousLuceneMultiSearcher");
					}
				})
				.then(indexSearchersMono);
		var localQueryParams = getLocalQueryParams(queryParams);

		return LLUtils.usingSendResource(indexSearchersSendResource,
				indexSearchers -> Mono.fromCallable(() -> {
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
							.skip(queryParams.offset())
							.take(queryParams.limit(), true);

					return new LuceneSearchResult(totalHitsCount, mergedFluxes, indexSearchers::close).send();
				}), false);
	}

	private LocalQueryParams getLocalQueryParams(LocalQueryParams queryParams) {
		return new LocalQueryParams(queryParams.query(),
				0,
				LuceneUtils.safeLongToInt((long) queryParams.offset() + (long) queryParams.limit()),
				queryParams.pageLimits(),
				queryParams.minCompetitiveScore(),
				queryParams.sort(),
				queryParams.scoreMode()
		);
	}
}
