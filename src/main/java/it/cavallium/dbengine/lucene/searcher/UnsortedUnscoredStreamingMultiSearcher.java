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
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.warp.commonutils.type.ShortNamedThreadFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink.OverflowStrategy;
import reactor.core.publisher.Mono;

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

							var shards = indexSearchers.shards();

							Flux<ScoreDoc> scoreDocsFlux = Flux.create(scoreDocsSink -> {
								var cmm = new ReactiveCollectorMultiManager(scoreDocsSink);

								AtomicInteger runningTasks = new AtomicInteger(0);

								runningTasks.addAndGet(shards.size());
								int mutableShardIndex = 0;
								for (IndexSearcher shard : shards) {
									int shardIndex = mutableShardIndex++;
									try {
										var collector = cmm.get(shardIndex);
										assert queryParams.complete() == cmm.scoreMode().isExhaustive();
										assert queryParams
												.getScoreModeOptional()
												.map(scoreMode -> scoreMode == cmm.scoreMode())
												.orElse(true);

										shard.search(localQueryParams.query(), collector);
									} catch (Throwable e) {
										scoreDocsSink.error(e);
									} finally {
										if (runningTasks.decrementAndGet() <= 0) {
											scoreDocsSink.complete();
										}
									}
								}
							}, OverflowStrategy.BUFFER);


							Flux<LLKeyScore> resultsFlux = LuceneUtils.convertHits(scoreDocsFlux, shards, keyFieldName, false);

							var totalHitsCount = new TotalHitsCount(0, false);
							Flux<LLKeyScore> mergedFluxes = resultsFlux
									.skip(queryParams2.offsetLong())
									.take(queryParams2.limitLong(), true);

							return new LuceneSearchResult(totalHitsCount, mergedFluxes, indexSearchers::close);
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
