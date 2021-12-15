package it.cavallium.dbengine.lucene.searcher;

import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

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
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
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
import reactor.util.function.Tuples;

public class UnsortedUnscoredStreamingMultiSearcher implements MultiSearcher {

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

				var cmm = new ReactiveCollectorMultiManager();

				Flux<ScoreDoc> scoreDocsFlux = Flux.fromIterable(shards)
						.index()
						.flatMap(tuple -> Flux.<ScoreDoc>create(scoreDocsSink -> {
							LLUtils.ensureBlocking();
							var index = toIntExact(requireNonNull(tuple.getT1()));
							var shard = tuple.getT2();
							var requested = new LongSemaphore(0);
							var collectorManager = cmm.get(requested, scoreDocsSink, index);

							assert queryParams.computePreciseHitsCount() == cmm.scoreMode().isExhaustive();

							scoreDocsSink.onRequest(requested::release);

							try {
								shard.search(localQueryParams.query(), collectorManager.newCollector());
								scoreDocsSink.complete();
							} catch (IOException e) {
								scoreDocsSink.error(e);
							}
						}, OverflowStrategy.BUFFER).subscribeOn(Schedulers.boundedElastic()));


				Flux<LLKeyScore> resultsFlux = LuceneUtils
						.convertHits(scoreDocsFlux.publishOn(Schedulers.boundedElastic()), shards, keyFieldName, false);

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
				queryParams.computePreciseHitsCount(),
				queryParams.timeout()
		);
	}

	@Override
	public String getName() {
		return "unsorted unscored streaming multi";
	}
}
