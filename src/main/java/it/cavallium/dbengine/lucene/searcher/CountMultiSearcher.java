package it.cavallium.dbengine.lucene.searcher;

import static it.cavallium.dbengine.client.UninterruptibleScheduler.uninterruptibleScheduler;
import static it.cavallium.dbengine.database.LLUtils.singleOrClose;
import static it.cavallium.dbengine.lucene.LuceneUtils.luceneScheduler;
import static it.cavallium.dbengine.lucene.searcher.GlobalQueryRewrite.NO_REWRITE;

import io.netty5.util.Send;
import it.cavallium.dbengine.client.query.current.data.TotalHitsCount;
import it.cavallium.dbengine.database.LLKeyScore;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.disk.LLIndexSearcher;
import it.cavallium.dbengine.database.disk.LLIndexSearchers;
import it.cavallium.dbengine.database.disk.LLIndexSearchers.UnshardedIndexSearchers;
import it.cavallium.dbengine.lucene.LuceneUtils;
import it.cavallium.dbengine.utils.SimpleResource;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.IndexSearcher;
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
		if (transformer != GlobalQueryRewrite.NO_REWRITE) {
			return LuceneUtils.rewriteMulti(this, indexSearchersMono, queryParams, keyFieldName, transformer);
		}
		if (queryParams.isSorted() && queryParams.limitLong() > 0) {
			throw new UnsupportedOperationException(
					"Sorted queries are not supported by SimpleUnsortedUnscoredLuceneMultiSearcher");
		}
		if (queryParams.needsScores() && queryParams.limitLong() > 0) {
			throw new UnsupportedOperationException(
					"Scored queries are not supported by SimpleUnsortedUnscoredLuceneMultiSearcher");
		}

		return Mono.usingWhen(indexSearchersMono, searchers -> Flux
				.fromIterable(searchers.llShards())
				.flatMap(searcher -> this.collect(Mono.just(searcher), queryParams, keyFieldName, transformer))
				.collectList()
				.map(results -> {
					boolean exactTotalHitsCount = true;
					long totalHitsCountValue = 0;
					for (LuceneSearchResult result : results) {
						exactTotalHitsCount &= result.totalHitsCount().exact();
						totalHitsCountValue += result.totalHitsCount().value();
						result.close();
					}

					var totalHitsCount = new TotalHitsCount(totalHitsCountValue, exactTotalHitsCount);

					return new LuceneSearchResult(totalHitsCount, Flux.empty());
				})
				.doOnDiscard(LuceneSearchResult.class, luceneSearchResult -> luceneSearchResult.close()),
				LLUtils::finalizeResource);
	}

	@Override
	public Mono<LuceneSearchResult> collect(Mono<LLIndexSearcher> indexSearcherMono,
			LocalQueryParams queryParams,
			@Nullable String keyFieldName,
			GlobalQueryRewrite transformer) {
		if (transformer != GlobalQueryRewrite.NO_REWRITE) {
			return LuceneUtils.rewrite(this, indexSearcherMono, queryParams, keyFieldName, transformer);
		}

		return Mono.usingWhen(indexSearcherMono, indexSearcher -> Mono.fromCallable(() -> {
					LLUtils.ensureBlocking();
					return (long) indexSearcher.getIndexSearcher().count(queryParams.query());
				}).subscribeOn(luceneScheduler()), LLUtils::finalizeResource)
				.publishOn(Schedulers.parallel())
				.transform(TimeoutUtil.timeoutMono(queryParams.timeout()))
				.map(count -> new LuceneSearchResult(TotalHitsCount.of(count, true), Flux.empty()));
	}

	@Override
	public String getName() {
		return "count";
	}
}
