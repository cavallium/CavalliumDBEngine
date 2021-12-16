package it.cavallium.dbengine.lucene.searcher;

import static it.cavallium.dbengine.lucene.LuceneUtils.withTimeout;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

import io.net5.buffer.api.Send;
import it.cavallium.dbengine.client.query.current.data.TotalHitsCount;
import it.cavallium.dbengine.database.LLKeyScore;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.disk.LLIndexSearchers;
import it.cavallium.dbengine.lucene.LuceneUtils;
import it.cavallium.dbengine.lucene.searcher.LLSearchTransformer.TransformerInput;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;
import org.warp.commonutils.type.ShortNamedThreadFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink.OverflowStrategy;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class UnsortedUnscoredStreamingMultiSearcher implements MultiSearcher {

	private static final int SEARCH_THREADS = Math.min(Math.max(8, Runtime.getRuntime().availableProcessors()), 128);
	private static final ThreadFactory THREAD_FACTORY = new ShortNamedThreadFactory("UnscoredStreamingSearcher");
	private static final ExecutorService EXECUTOR = Executors.newCachedThreadPool(); // Executors.newFixedThreadPool(SEARCH_THREADS, THREAD_FACTORY);

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

				Flux<ScoreDoc> scoreDocsFlux = getScoreDocs(localQueryParams, shards);

				Flux<LLKeyScore> resultsFlux = LuceneUtils.convertHits(scoreDocsFlux, shards, keyFieldName, false);

				var totalHitsCount = new TotalHitsCount(0, false);
				Flux<LLKeyScore> mergedFluxes = resultsFlux
						.skip(queryParams2.offsetLong())
						.take(queryParams2.limitLong(), true);

				return new LuceneSearchResult(totalHitsCount, mergedFluxes, indexSearchers::close);
			});
		}, false);
	}

	private Flux<ScoreDoc> getScoreDocs(LocalQueryParams localQueryParams, List<IndexSearcher> shards) {
		return Flux.<ScoreDoc>create(sink -> {
			AtomicReference<Thread> threadAtomicReference = new AtomicReference<>();

			var disposable = EXECUTOR.submit(() -> {
				try {
					LLUtils.ensureBlocking();
					threadAtomicReference.set(Thread.currentThread());
					int shardIndexTemp = 0;
					for (IndexSearcher shard : shards) {
						if (sink.isCancelled()) break;
						final int shardIndex = shardIndexTemp;
						var collector = withTimeout(new SimpleCollector() {
							private LeafReaderContext leafReaderContext;

							@Override
							protected void doSetNextReader(LeafReaderContext context) {
								this.leafReaderContext = context;
							}

							@Override
							public void collect(int i) {
								// Assert that this is a non-blocking context
								assert !Schedulers.isInNonBlockingThread();
								var scoreDoc = new ScoreDoc(leafReaderContext.docBase + i, 0, shardIndex);
								while (sink.requestedFromDownstream() <= 0 || sink.isCancelled()) {
									if (sink.isCancelled()) {
										throw new CollectionTerminatedException();
									}
									// 1000ms
									LockSupport.parkNanos(1000000000L);
								}
								sink.next(scoreDoc);
							}

							@Override
							public ScoreMode scoreMode() {
								return ScoreMode.COMPLETE_NO_SCORES;
							}
						}, localQueryParams.timeout());
						shard.search(localQueryParams.query(), collector);
						shardIndexTemp++;
					}
				} catch (Throwable e) {
					sink.error(e);
				}
				sink.complete();
			});
			sink.onRequest(lc -> LockSupport.unpark(threadAtomicReference.get()));
			sink.onDispose(() -> disposable.cancel(false));
		}, OverflowStrategy.BUFFER).publishOn(Schedulers.boundedElastic());

	}

	private LocalQueryParams getLocalQueryParams(LocalQueryParams queryParams) {
		return new LocalQueryParams(queryParams.query(),
				0L,
				queryParams.offsetLong() + queryParams.limitLong(),
				queryParams.pageLimits(),
				queryParams.minCompetitiveScore(),
				queryParams.sort(),
				queryParams.computePreciseHitsCount(),
				queryParams.timeout()
		);
	}

	@Override
	public String getName() {
		return "unsorted unscored streaming multi";
	}
}
