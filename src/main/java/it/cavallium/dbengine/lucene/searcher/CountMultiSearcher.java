package it.cavallium.dbengine.lucene.searcher;

import static it.cavallium.dbengine.client.UninterruptibleScheduler.uninterruptibleScheduler;

import io.net5.buffer.api.Send;
import it.cavallium.dbengine.client.query.current.data.TotalHitsCount;
import it.cavallium.dbengine.database.LLKeyScore;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.disk.LLIndexSearcher;
import it.cavallium.dbengine.database.disk.LLIndexSearchers;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class CountMultiSearcher implements MultiSearcher {


	@Override
	public Mono<LuceneSearchResult> collectMulti(Mono<Send<LLIndexSearchers>> indexSearchersMono,
			LocalQueryParams queryParams,
			String keyFieldName,
			GlobalQueryRewrite transformer) {
		Mono<LocalQueryParams> queryParamsMono;
		if (transformer == GlobalQueryRewrite.NO_REWRITE) {
			queryParamsMono = Mono.just(queryParams);
		} else {
			queryParamsMono = indexSearchersMono
					.publishOn(uninterruptibleScheduler(Schedulers.boundedElastic()))
					.handle((indexSearchers, sink) -> {
						try {
							sink.next(transformer.rewrite(indexSearchers.receive(), queryParams));
						} catch (IOException ex) {
							sink.error(ex);
						}
					});
		}

		return queryParamsMono.flatMap(queryParams2 -> LLUtils.usingSendResource(indexSearchersMono, indexSearchers -> {
			var localQueryParams = getLocalQueryParams(queryParams2);
			return Mono.fromRunnable(() -> {
				LLUtils.ensureBlocking();
				if (queryParams2.isSorted() && queryParams2.limitLong() > 0) {
					throw new UnsupportedOperationException(
							"Sorted queries are not supported by SimpleUnsortedUnscoredLuceneMultiSearcher");
				}
				if (queryParams2.needsScores() && queryParams2.limitLong() > 0) {
					throw new UnsupportedOperationException(
							"Scored queries are not supported by SimpleUnsortedUnscoredLuceneMultiSearcher");
				}
			}).thenMany(Flux.fromIterable(indexSearchers.shards())).flatMap(searcher -> {
				var llSearcher = Mono.fromCallable(() -> new LLIndexSearcher(searcher, false, null).send());
				return this.collect(llSearcher, localQueryParams, keyFieldName, transformer);
			}).collectList().map(results -> {
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
						luceneSearchResult.close();
					}
					indexSearchers.close();
				});
			});
		}, false));
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
	public Mono<LuceneSearchResult> collect(Mono<Send<LLIndexSearcher>> indexSearcherMono,
			LocalQueryParams queryParams,
			String keyFieldName,
			GlobalQueryRewrite transformer) {
		return Mono
				.usingWhen(
						indexSearcherMono,
						indexSearcher -> {
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
										try (var is = indexSearcher.receive()) {
											LLUtils.ensureBlocking();

											return (long) is.getIndexSearcher().count(queryParams2.query());
										}
									}).subscribeOn(uninterruptibleScheduler(Schedulers.boundedElastic())))
									.publishOn(Schedulers.parallel())
									.transform(TimeoutUtil.timeoutMono(queryParams.timeout()));
						},
						is -> Mono.empty()
				)
				.map(count -> new LuceneSearchResult(TotalHitsCount.of(count, true), Flux.empty(), null));
	}

	@Override
	public String getName() {
		return "count";
	}
}
