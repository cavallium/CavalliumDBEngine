package it.cavallium.dbengine.lucene.searcher;

import static com.google.common.collect.Streams.mapWithIndex;
import static it.cavallium.dbengine.utils.StreamUtils.toList;

import it.cavallium.dbengine.client.query.current.data.TotalHitsCount;
import it.cavallium.dbengine.database.LLKeyScore;
import it.cavallium.dbengine.database.disk.LLIndexSearchers;
import it.cavallium.dbengine.lucene.LuceneCloseable;
import it.cavallium.dbengine.lucene.LuceneUtils;
import java.io.IOException;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.jetbrains.annotations.Nullable;

public class UnsortedStreamingMultiSearcher implements MultiSearcher {


	protected static final Logger LOG = LogManager.getLogger(UnsortedStreamingMultiSearcher.class);

	@Override
	public LuceneSearchResult collectMulti(LLIndexSearchers indexSearchers,
			LocalQueryParams queryParams,
			@Nullable String keyFieldName,
			GlobalQueryRewrite transformer,
			Function<Stream<LLKeyScore>, Stream<LLKeyScore>> filterer) {
		if (transformer != GlobalQueryRewrite.NO_REWRITE) {
			return LuceneUtils.rewriteMulti(this, indexSearchers, queryParams, keyFieldName, transformer, filterer);
		}
		if (queryParams.isSorted() && queryParams.limitLong() > 0) {
			throw new UnsupportedOperationException("Sorted queries are not supported" + " by UnsortedContinuousLuceneMultiSearcher");
		}
		var localQueryParams = getLocalQueryParams(queryParams);

		var shards = indexSearchers.shards();

		Stream<ScoreDoc> scoreDocsFlux = getScoreDocs(localQueryParams, shards);

		Stream<LLKeyScore> resultsFlux = LuceneUtils.convertHits(scoreDocsFlux, shards, keyFieldName);

		var totalHitsCount = new TotalHitsCount(0, false);
		Stream<LLKeyScore> mergedFluxes = resultsFlux.skip(queryParams.offsetLong()).limit(queryParams.limitLong());

		return new LuceneSearchResult(totalHitsCount, toList(filterer.apply(mergedFluxes)));
	}

	private Stream<ScoreDoc> getScoreDocs(LocalQueryParams localQueryParams, List<IndexSearcher> shards) {
		return mapWithIndex(shards.stream(),
				(shard, shardIndex) -> LuceneGenerator.reactive(shard, localQueryParams, (int) shardIndex))
				.flatMap(Function.identity());
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
	public String toString() {
		return "unsorted streaming multi";
	}
}
