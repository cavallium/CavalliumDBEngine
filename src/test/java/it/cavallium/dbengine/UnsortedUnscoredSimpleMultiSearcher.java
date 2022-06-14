package it.cavallium.dbengine;

import static it.cavallium.dbengine.client.UninterruptibleScheduler.uninterruptibleScheduler;
import static it.cavallium.dbengine.lucene.searcher.GlobalQueryRewrite.NO_REWRITE;

import io.netty5.buffer.api.Send;
import it.cavallium.dbengine.client.query.current.data.TotalHitsCount;
import it.cavallium.dbengine.database.LLKeyScore;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.disk.LLIndexSearcher;
import it.cavallium.dbengine.database.disk.LLIndexSearchers;
import it.cavallium.dbengine.lucene.searcher.GlobalQueryRewrite;
import it.cavallium.dbengine.lucene.searcher.LocalQueryParams;
import it.cavallium.dbengine.lucene.searcher.LocalSearcher;
import it.cavallium.dbengine.lucene.searcher.LuceneSearchResult;
import it.cavallium.dbengine.lucene.searcher.MultiSearcher;
import it.cavallium.dbengine.utils.SimpleResource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class UnsortedUnscoredSimpleMultiSearcher implements MultiSearcher {

	private static final Logger LOG = LogManager.getLogger(UnsortedUnscoredSimpleMultiSearcher.class);

	private final LocalSearcher localSearcher;

	public UnsortedUnscoredSimpleMultiSearcher(LocalSearcher localSearcher) {
		this.localSearcher = localSearcher;
	}

	@Override
	public Mono<LuceneSearchResult> collectMulti(Mono<LLIndexSearchers> indexSearchersMono,
			LocalQueryParams queryParams,
			String keyFieldName,
			GlobalQueryRewrite transformer) {

		return indexSearchersMono.flatMap(indexSearchers -> {
			Mono<LocalQueryParams> queryParamsMono;
			if (transformer == NO_REWRITE) {
				queryParamsMono = Mono.just(queryParams);
			} else {
				queryParamsMono = Mono
						.fromCallable(() -> transformer.rewrite(indexSearchers, queryParams))
						.subscribeOn(uninterruptibleScheduler(Schedulers.boundedElastic()));
			}

			return queryParamsMono.flatMap(queryParams2 -> {
						var localQueryParams = getLocalQueryParams(queryParams2);
						return Flux
								.fromIterable(indexSearchers.llShards())
								.flatMap(searcher ->
										localSearcher.collect(Mono.just(searcher), localQueryParams, keyFieldName, transformer))
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
										resultsToDrop.forEach(SimpleResource::close);
										try {
											indexSearchers.close();
										} catch (IOException e) {
											LOG.error("Can't close index searchers", e);
										}
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
	public String getName() {
		return "unsorted unscored simple multi";
	}
}
