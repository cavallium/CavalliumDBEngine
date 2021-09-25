package it.cavallium.dbengine.lucene.searcher;

import io.net5.buffer.api.Resource;
import io.net5.buffer.api.Send;
import it.cavallium.dbengine.client.query.current.data.TotalHitsCount;
import it.cavallium.dbengine.database.LLKeyScore;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.disk.LLIndexSearcher;
import it.cavallium.dbengine.database.disk.LLIndexSearchers;
import it.cavallium.dbengine.lucene.LuceneUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;
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

					Many<ScoreDoc> scoreDocsSink = Sinks.many().unicast().onBackpressureBuffer(QUEUE_SUPPLIER.get());

					var cm = new CollectorManager<Collector, Void>() {

						class IterableCollector implements Collector {

							private int shardIndex;

							@Override
							public LeafCollector getLeafCollector(LeafReaderContext leafReaderContext) throws IOException {
								return new LeafCollector() {
									@Override
									public void setScorer(Scorable scorable) throws IOException {

									}

									@Override
									public void collect(int i) throws IOException {
										if (Schedulers.isInNonBlockingThread()) {
											throw new UnsupportedOperationException("Called collect in a nonblocking thread");
										}
										var scoreDoc = new ScoreDoc(leafReaderContext.docBase + i, 0, shardIndex);
										boolean shouldRetry;
										do {
											var currentError = scoreDocsSink.tryEmitNext(scoreDoc);
											shouldRetry = currentError == EmitResult.FAIL_NON_SERIALIZED
													|| currentError == EmitResult.FAIL_OVERFLOW
													|| currentError == EmitResult.FAIL_ZERO_SUBSCRIBER;
											if (shouldRetry) {
												LockSupport.parkNanos(10);
											}
											if (!shouldRetry && currentError.isFailure()) {
												currentError.orThrow();
											}
										} while (shouldRetry);
									}
								};
							}

							@Override
							public ScoreMode scoreMode() {
								return ScoreMode.COMPLETE_NO_SCORES;
							}

							public void setShardIndex(int shardIndex) {
								this.shardIndex = shardIndex;
							}
						}

						@Override
						public IterableCollector newCollector() {
							return new IterableCollector();
						}

						@Override
						public Void reduce(Collection<Collector> collection) {
							throw new UnsupportedOperationException();
						}
					};

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

					return new LuceneSearchResult(totalHitsCount, mergedFluxes, d -> {
						indexSearchers.close();
					}).send();
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
