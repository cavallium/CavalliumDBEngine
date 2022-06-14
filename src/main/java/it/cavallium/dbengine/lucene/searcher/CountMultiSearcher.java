package it.cavallium.dbengine.lucene.searcher;

import static it.cavallium.dbengine.client.UninterruptibleScheduler.uninterruptibleScheduler;
import static it.cavallium.dbengine.database.LLUtils.singleOrClose;

import io.netty5.buffer.api.Send;
import it.cavallium.dbengine.client.query.current.data.TotalHitsCount;
import it.cavallium.dbengine.database.LLKeyScore;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.disk.LLIndexSearcher;
import it.cavallium.dbengine.database.disk.LLIndexSearchers;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class CountMultiSearcher implements MultiSearcher {

	protected static final Logger LOG = LogManager.getLogger(CountMultiSearcher.class);

	@Override
	public Mono<LuceneSearchResult> collectMulti(Mono<LLIndexSearchers> indexSearchersMono,
			LocalQueryParams queryParams,
			String keyFieldName,
			GlobalQueryRewrite transformer) {
		return singleOrClose(indexSearchersMono, indexSearchers -> {
			Mono<LocalQueryParams> queryParamsMono;
			if (transformer == GlobalQueryRewrite.NO_REWRITE) {
				queryParamsMono = Mono.just(queryParams);
			} else {
				queryParamsMono = Mono
						.fromCallable(() -> transformer.rewrite(indexSearchers, queryParams))
						.subscribeOn(uninterruptibleScheduler(Schedulers.boundedElastic()));
			}

			return queryParamsMono.flatMap(queryParams2 -> {
				var localQueryParams = getLocalQueryParams(queryParams2);
				return Mono
						.fromRunnable(() -> {
							if (queryParams2.isSorted() && queryParams2.limitLong() > 0) {
								throw new UnsupportedOperationException(
										"Sorted queries are not supported by SimpleUnsortedUnscoredLuceneMultiSearcher");
							}
							if (queryParams2.needsScores() && queryParams2.limitLong() > 0) {
								throw new UnsupportedOperationException(
										"Scored queries are not supported by SimpleUnsortedUnscoredLuceneMultiSearcher");
							}
						})
						.thenMany(Flux.fromIterable(indexSearchers.llShards()))
						.flatMap(searcher -> this.collect(Mono.just(searcher), localQueryParams, keyFieldName, transformer))
						.collectList()
						.map(results -> {
							List<LuceneSearchResult> resultsToDrop = new ArrayList<>(results.size());
							List<Flux<LLKeyScore>> resultsFluxes = new ArrayList<>(results.size());
							boolean exactTotalHitsCount = true;
							long totalHitsCountValue = 0;
							for (LuceneSearchResult result : results) {
								resultsToDrop.add(result);
								resultsFluxes.add(result.results());
								exactTotalHitsCount &= result.totalHitsCount().exact();
								totalHitsCountValue += result.totalHitsCount().value();
							}

							var totalHitsCount = new TotalHitsCount(totalHitsCountValue, exactTotalHitsCount);
							Flux<LLKeyScore> mergedFluxes = Flux
									.merge(resultsFluxes)
									.skip(queryParams2.offsetLong())
									.take(queryParams2.limitLong(), true);

							return new LuceneSearchResult(totalHitsCount, mergedFluxes, () -> {
								for (LuceneSearchResult luceneSearchResult : resultsToDrop) {
									if (luceneSearchResult.isAccessible()) {
										luceneSearchResult.close();
									}
								}
								try {
									indexSearchers.close();
								} catch (IOException e) {
									LOG.error("Can't close index searchers", e);
								}
							});
						});
			});
		});
	}

	private LocalQueryParams getLocalQueryParams(LocalQueryParams queryParams) {
		return new LocalQueryParams(queryParams.query(),
				0L,
				queryParams.offsetLong() + queryParams.limitLong(),
				queryParams.pageLimits(),
				queryParams.sort(),
				queryParams.computePreciseHitsCount(),
				queryParams.timeout()
		);
	}

	@Override
	public Mono<LuceneSearchResult> collect(Mono<LLIndexSearcher> indexSearcherMono,
			LocalQueryParams queryParams,
			@Nullable String keyFieldName,
			GlobalQueryRewrite transformer) {
		return singleOrClose(indexSearcherMono, indexSearcher -> {
			Mono<LocalQueryParams> queryParamsMono;
			if (transformer == GlobalQueryRewrite.NO_REWRITE) {
				queryParamsMono = Mono.just(queryParams);
			} else {
				queryParamsMono = Mono
						.fromCallable(() -> transformer.rewrite(LLIndexSearchers.unsharded(indexSearcher), queryParams))
						.subscribeOn(uninterruptibleScheduler(Schedulers.boundedElastic()));
			}

			return queryParamsMono
					.flatMap(queryParams2 -> Mono.fromCallable(() -> {
						LLUtils.ensureBlocking();
						return (long) indexSearcher.getIndexSearcher().count(queryParams2.query());
					}).subscribeOn(uninterruptibleScheduler(Schedulers.boundedElastic())))
					.publishOn(Schedulers.parallel())
					.transform(TimeoutUtil.timeoutMono(queryParams.timeout()))
					.map(count -> new LuceneSearchResult(TotalHitsCount.of(count, true), Flux.empty(), () -> {
						try {
							indexSearcher.close();
						} catch (IOException e) {
							LOG.error("Can't close index searchers", e);
						}
					}));
		});
	}

	@Override
	public String getName() {
		return "count";
	}
}
