package it.cavallium.dbengine;

import io.net5.buffer.api.Send;
import it.cavallium.dbengine.client.query.current.data.TotalHitsCount;
import it.cavallium.dbengine.database.LLKeyScore;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.disk.LLIndexSearcher;
import it.cavallium.dbengine.database.disk.LLIndexSearchers;
import it.cavallium.dbengine.lucene.searcher.LLSearchTransformer;
import it.cavallium.dbengine.lucene.searcher.LLSearchTransformer.TransformerInput;
import it.cavallium.dbengine.lucene.searcher.LocalQueryParams;
import it.cavallium.dbengine.lucene.searcher.LocalSearcher;
import it.cavallium.dbengine.lucene.searcher.LuceneSearchResult;
import it.cavallium.dbengine.lucene.searcher.MultiSearcher;
import java.util.ArrayList;
import java.util.List;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class UnsortedUnscoredSimpleMultiSearcher implements MultiSearcher {

	private final LocalSearcher localSearcher;

	public UnsortedUnscoredSimpleMultiSearcher(LocalSearcher localSearcher) {
		this.localSearcher = localSearcher;
	}

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
						var transformerInput = Mono.just(new TransformerInput(indexSearchers, queryParams));
						queryParamsMono = transformer.transform(transformerInput);
					}

					return queryParamsMono.flatMap(queryParams2 -> {
						var localQueryParams = getLocalQueryParams(queryParams2);
						return Flux
								.fromIterable(indexSearchers.shards())
								.flatMap(searcher -> {
									var llSearcher = Mono.fromCallable(() -> new LLIndexSearcher(searcher, false, null).send());
									return localSearcher.collect(llSearcher, localQueryParams, keyFieldName, transformer);
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
								})
								.doFirst(() -> {
									LLUtils.ensureBlocking();
									if (queryParams2.isSorted() && queryParams2.limitLong() > 0) {
										throw new UnsupportedOperationException("Sorted queries are not supported"
												+ " by SimpleUnsortedUnscoredLuceneMultiSearcher");
									}
									if (queryParams2.needsScores() && queryParams2.limitLong() > 0) {
										throw new UnsupportedOperationException("Scored queries are not supported"
												+ " by SimpleUnsortedUnscoredLuceneMultiSearcher");
									}
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
				queryParams.computePreciseHitsCount(),
				queryParams.timeout()
		);
	}

	@Override
	public String getName() {
		return "unsorted unscored simple multi";
	}
}
