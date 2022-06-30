package it.cavallium.dbengine.lucene.searcher;

import static it.cavallium.dbengine.client.UninterruptibleScheduler.uninterruptibleScheduler;
import static it.cavallium.dbengine.database.LLUtils.singleOrClose;

import io.netty5.buffer.api.Send;
import it.cavallium.dbengine.client.query.current.data.TotalHitsCount;
import it.cavallium.dbengine.database.LLKeyScore;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.disk.LLIndexSearchers;
import it.cavallium.dbengine.lucene.LuceneUtils;
import it.cavallium.dbengine.lucene.MaxScoreAccumulator;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import it.cavallium.dbengine.lucene.hugepq.search.CustomHitsThresholdChecker;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class UnsortedStreamingMultiSearcher implements MultiSearcher {


	protected static final Logger LOG = LogManager.getLogger(UnsortedStreamingMultiSearcher.class);

	@Override
	public Mono<LuceneSearchResult> collectMulti(Mono<LLIndexSearchers> indexSearchersMono,
			LocalQueryParams queryParams,
			@Nullable String keyFieldName,
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

			return queryParamsMono.map(queryParams2 -> {
				var localQueryParams = getLocalQueryParams(queryParams2);
				if (queryParams2.isSorted() && queryParams2.limitLong() > 0) {
					throw new UnsupportedOperationException("Sorted queries are not supported"
							+ " by UnsortedContinuousLuceneMultiSearcher");
				}
				var shards = indexSearchers.shards();

				Flux<ScoreDoc> scoreDocsFlux = getScoreDocs(localQueryParams, shards);

				Flux<LLKeyScore> resultsFlux = LuceneUtils.convertHits(scoreDocsFlux, shards, keyFieldName, false);

				var totalHitsCount = new TotalHitsCount(0, false);
				Flux<LLKeyScore> mergedFluxes = resultsFlux
						.skip(queryParams2.offsetLong())
						.take(queryParams2.limitLong(), true);

				return new LuceneSearchResult(totalHitsCount, mergedFluxes, () -> {
					try {
						indexSearchers.close();
					} catch (UncheckedIOException e) {
						LOG.error("Can't close index searchers", e);
					}
				});
			});
		});
	}

	private Flux<ScoreDoc> getScoreDocs(LocalQueryParams localQueryParams, List<IndexSearcher> shards) {
		return Flux.defer(() -> {
			var hitsThreshold = CustomHitsThresholdChecker.createShared(localQueryParams.getTotalHitsThresholdLong());
			MaxScoreAccumulator maxScoreAccumulator = new MaxScoreAccumulator();
			return Flux.fromIterable(shards).index().flatMap(tuple -> {
				var shardIndex = (int) (long) tuple.getT1();
				var shard = tuple.getT2();
				return LuceneGenerator.reactive(shard, localQueryParams, shardIndex);
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
	public String getName() {
		return "unsorted streaming multi";
	}
}
