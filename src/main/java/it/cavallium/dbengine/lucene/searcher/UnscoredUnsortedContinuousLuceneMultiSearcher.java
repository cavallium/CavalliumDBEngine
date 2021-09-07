package it.cavallium.dbengine.lucene.searcher;

import it.cavallium.dbengine.client.query.current.data.TotalHitsCount;
import it.cavallium.dbengine.lucene.LuceneUtils;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.IndexSearcher;
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

public class UnscoredUnsortedContinuousLuceneMultiSearcher implements LuceneMultiSearcher {

	private static final Scheduler UNSCORED_UNSORTED_EXECUTOR = Schedulers.newBoundedElastic(Runtime
			.getRuntime()
			.availableProcessors(), Schedulers.DEFAULT_BOUNDED_ELASTIC_QUEUESIZE, "UnscoredUnsortedExecutor");

	@Override
	public Mono<LuceneShardSearcher> createShardSearcher(LocalQueryParams queryParams) {
		return Mono
				.fromCallable(() -> {
					AtomicBoolean alreadySubscribed = new AtomicBoolean(false);
					Many<ScoreDoc> scoreDocsSink = Sinks.many().unicast().onBackpressureBuffer();
					// 1 is the collect phase
					AtomicInteger remainingCollectors = new AtomicInteger(1);

					if (queryParams.isScored()) {
						throw new UnsupportedOperationException("Can't use the unscored searcher to do a scored or sorted query");
					}

					var cm = new CollectorManager<Collector, Void>() {

						class IterableCollector extends SimpleCollector {

							private int shardIndex;
							private LeafReaderContext context;

							@Override
							public void collect(int i) {
								if (Schedulers.isInNonBlockingThread()) {
									throw new UnsupportedOperationException("Called collect in a nonblocking thread");
								}
								var scoreDoc = new ScoreDoc(context.docBase + i, 0, shardIndex);
								synchronized (scoreDocsSink) {
									while (scoreDocsSink.tryEmitNext(scoreDoc) == EmitResult.FAIL_OVERFLOW) {
										LockSupport.parkNanos(10);
									}
								}
							}

							@Override
							protected void doSetNextReader(LeafReaderContext context) {
								this.context = context;
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

					return new LuceneShardSearcher() {
						private final Object lock = new Object();
						private final List<IndexSearcher> indexSearchersArray = new ArrayList<>();
						private final List<Mono<Void>> indexSearcherReleasersArray = new ArrayList<>();
						@Override
						public Mono<Void> searchOn(IndexSearcher indexSearcher,
								Mono<Void> releaseIndexSearcher,
								LocalQueryParams queryParams,
								Scheduler scheduler) {
							return Mono
									.fromCallable(() -> {
										if (Schedulers.isInNonBlockingThread()) {
											throw new UnsupportedOperationException("Called searchOn in a nonblocking thread");
										}
										var collector = cm.newCollector();
										int collectorShardIndex;
										synchronized (lock) {
											collectorShardIndex = indexSearchersArray.size();
											indexSearchersArray.add(indexSearcher);
											indexSearcherReleasersArray.add(releaseIndexSearcher);
										}
										collector.setShardIndex(collectorShardIndex);
										remainingCollectors.incrementAndGet();
										UNSCORED_UNSORTED_EXECUTOR.schedule(() -> {
											try {
												indexSearcher.search(queryParams.query(), collector);

												synchronized (scoreDocsSink) {
													decrementRemainingCollectors(scoreDocsSink, remainingCollectors);
												}
											} catch (IOException e) {
												scoreDocsSink.tryEmitError(e);
											}
										});
										return null;
									})
									.subscribeOn(scheduler);
						}

						@Override
						public Mono<LuceneSearchResult> collect(LocalQueryParams queryParams,
								String keyFieldName,
								Scheduler scheduler) {
							return Mono
									.fromCallable(() -> {
										if (Schedulers.isInNonBlockingThread()) {
											throw new UnsupportedOperationException("Called collect in a nonblocking thread");
										}
										synchronized (scoreDocsSink) {
											decrementRemainingCollectors(scoreDocsSink, remainingCollectors);
										}

										if (!alreadySubscribed.compareAndSet(false, true)) {
											throw new UnsupportedOperationException("Already subscribed!");
										}

										IndexSearchers indexSearchers;
										Mono<Void> release;
										synchronized (lock) {
											indexSearchers = IndexSearchers.of(indexSearchersArray);
											release = Mono.when(indexSearcherReleasersArray);
										}

										AtomicBoolean resultsAlreadySubscribed = new AtomicBoolean(false);

										var resultsFlux = Mono
												.<Void>fromCallable(() -> {
													if (!resultsAlreadySubscribed.compareAndSet(false, true)) {
														throw new UnsupportedOperationException("Already subscribed!");
													}
													return null;
												})
												.thenMany(scoreDocsSink.asFlux())
												.buffer(1024, ObjectArrayList::new)
												.flatMap(scoreDocs -> LuceneUtils.convertHits(scoreDocs.toArray(ScoreDoc[]::new),
														indexSearchers,
														keyFieldName,
														scheduler,
														false
												));

										return new LuceneSearchResult(TotalHitsCount.of(0, false), resultsFlux, release);
									});
						}
					};
				});
	}

	private static void decrementRemainingCollectors(Many<ScoreDoc> scoreDocsSink, AtomicInteger remainingCollectors) {
		if (remainingCollectors.decrementAndGet() <= 0) {
			scoreDocsSink.tryEmitComplete();
		}
	}
}
