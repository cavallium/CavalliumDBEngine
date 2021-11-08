package it.cavallium.dbengine.lucene.searcher;

import io.net5.buffer.api.Resource;
import io.net5.buffer.api.Send;
import it.cavallium.dbengine.client.query.current.data.TotalHitsCount;
import it.cavallium.dbengine.database.LLKeyScore;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.disk.LLIndexSearcher;
import it.cavallium.dbengine.database.disk.LLIndexSearchers;
import it.cavallium.dbengine.lucene.searcher.LLSearchTransformer.TransformerInput;
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
			LLSearchTransformer transformer) {

		return LLUtils.usingSendResource(indexSearchersMono,
				indexSearchers -> {
					Mono<LocalQueryParams> queryParamsMono;
					if (transformer == LLSearchTransformer.NO_TRANSFORMATION) {
						queryParamsMono = Mono.just(queryParams);
					} else {
						queryParamsMono = transformer.transform(Mono
								.fromSupplier(() -> new TransformerInput(indexSearchers, queryParams)));
					}

					return queryParamsMono.flatMap(queryParams2 -> {
								var localQueryParams = getLocalQueryParams(queryParams2);
								return Mono
										.fromRunnable(() -> {
											LLUtils.ensureBlocking();
											if (queryParams2.isSorted() && queryParams2.limitLong() > 0) {
												throw new UnsupportedOperationException("Sorted queries are not supported"
														+ " by SimpleUnsortedUnscoredLuceneMultiSearcher");
											}
											if (queryParams2.needsScores() && queryParams2.limitLong() > 0) {
												throw new UnsupportedOperationException("Scored queries are not supported"
														+ " by SimpleUnsortedUnscoredLuceneMultiSearcher");
											}
										})
										.thenMany(Flux.fromIterable(indexSearchers.shards()))
										.flatMap(searcher -> {
											var llSearcher = Mono.fromCallable(() -> new LLIndexSearcher(searcher, false, null).send());
											return this.collect(llSearcher, localQueryParams, keyFieldName, transformer);
										})
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
													luceneSearchResult.close();
												}
												indexSearchers.close();
											});
										});
							}
					);
				},
				false
		);
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
	public Mono<LuceneSearchResult> collect(Mono<Send<LLIndexSearcher>> indexSearcherMono,
			LocalQueryParams queryParams,
			String keyFieldName,
			LLSearchTransformer transformer) {
		return Mono
				.usingWhen(
						indexSearcherMono,
						indexSearcher -> {
							Mono<LocalQueryParams> queryParamsMono;
							if (transformer == LLSearchTransformer.NO_TRANSFORMATION) {
								queryParamsMono = Mono.just(queryParams);
							} else {
								queryParamsMono = transformer.transform(Mono
										.fromSupplier(() -> new TransformerInput(LLIndexSearchers.unsharded(indexSearcher), queryParams)));
							}

							return queryParamsMono.flatMap(queryParams2 -> Mono.fromCallable(() -> {
								try (var is = indexSearcher.receive()) {
									LLUtils.ensureBlocking();
									return is.getIndexSearcher().count(queryParams2.query());
								}
							}).subscribeOn(Schedulers.boundedElastic()));
						},
						is -> Mono.empty()
				)
				.map(count -> new LuceneSearchResult(TotalHitsCount.of(count, true), Flux.empty(), null))
				.doOnDiscard(Send.class, Send::close)
				.doOnDiscard(Resource.class, Resource::close);
	}

	@Override
	public String getName() {
		return "count";
	}
}
