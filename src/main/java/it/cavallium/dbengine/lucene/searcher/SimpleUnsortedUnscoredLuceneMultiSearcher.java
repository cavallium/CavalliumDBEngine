package it.cavallium.dbengine.lucene.searcher;

import io.net5.buffer.api.Send;
import it.cavallium.dbengine.client.query.current.data.TotalHitsCount;
import it.cavallium.dbengine.database.LLKeyScore;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.disk.LLIndexSearcher;
import it.cavallium.dbengine.database.disk.LLIndexSearchers;
import java.util.ArrayList;
import java.util.List;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class SimpleUnsortedUnscoredLuceneMultiSearcher implements LuceneMultiSearcher {

	private final LuceneLocalSearcher localSearcher;

	public SimpleUnsortedUnscoredLuceneMultiSearcher(LuceneLocalSearcher localSearcher) {
		this.localSearcher = localSearcher;
	}

	@Override
	public Mono<Send<LuceneSearchResult>> collectMulti(Mono<Send<LLIndexSearchers>> indexSearchersMono,
			LocalQueryParams queryParams,
			String keyFieldName,
			LLSearchTransformer transformer) {
		var indexSearchersResource = Mono
				.fromRunnable(() -> {
					LLUtils.ensureBlocking();
					if (queryParams.isSorted() && queryParams.limit() > 0) {
						throw new UnsupportedOperationException("Sorted queries are not supported"
								+ " by SimpleUnsortedUnscoredLuceneMultiSearcher");
					}
					if (queryParams.isScored() && queryParams.limit() > 0) {
						throw new UnsupportedOperationException("Scored queries are not supported"
								+ " by SimpleUnsortedUnscoredLuceneMultiSearcher");
					}
				})
				.then(indexSearchersMono.map(Send::receive));
		var localQueryParams = getLocalQueryParams(queryParams);

		return LLUtils.usingResource(indexSearchersResource,
				indexSearchers -> Flux.fromIterable(indexSearchers.shards())
						.flatMap(searcher -> {
							var llSearcher = Mono.fromCallable(() -> new LLIndexSearcher(searcher, d -> {}).send());
							return localSearcher.collect(llSearcher, localQueryParams, keyFieldName, transformer);
						})
						.collectList()
						.map(results -> {
							List<LuceneSearchResult> resultsToDrop = new ArrayList<>(results.size());
							List<Flux<LLKeyScore>> resultsFluxes = new ArrayList<>(results.size());
							boolean exactTotalHitsCount = true;
							long totalHitsCountValue = 0;
							for (Send<LuceneSearchResult> resultToReceive : results) {
								LuceneSearchResult result = resultToReceive.receive();
								resultsToDrop.add(result);
								resultsFluxes.add(result.results());
								exactTotalHitsCount &= result.totalHitsCount().exact();
								totalHitsCountValue += result.totalHitsCount().value();
							}

							var totalHitsCount = new TotalHitsCount(totalHitsCountValue, exactTotalHitsCount);
							Flux<LLKeyScore> mergedFluxes = Flux
									.merge(resultsFluxes)
									.skip(queryParams.offset())
									.take(queryParams.limit(), true);

							return new LuceneSearchResult(totalHitsCount, mergedFluxes, d -> {
								for (LuceneSearchResult luceneSearchResult : resultsToDrop) {
									luceneSearchResult.close();
								}
							}).send();
						}),
				false
		);
	}

	private LocalQueryParams getLocalQueryParams(LocalQueryParams queryParams) {
		return new LocalQueryParams(queryParams.query(),
				0,
				queryParams.limit(),
				queryParams.pageLimits(),
				queryParams.minCompetitiveScore(),
				queryParams.sort(),
				queryParams.scoreMode()
		);
	}
}
