package it.cavallium.dbengine.lucene.searcher;

import it.cavallium.dbengine.client.query.current.data.TotalHitsCount;
import it.cavallium.dbengine.database.LLKeyScore;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.disk.LLIndexSearcher;
import it.cavallium.dbengine.database.disk.LLIndexSearchers;
import it.cavallium.dbengine.lucene.LuceneUtils;
import it.cavallium.dbengine.utils.DBException;
import java.io.IOException;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.QueryTimeout;
import org.apache.lucene.index.QueryTimeoutImpl;
import org.jetbrains.annotations.Nullable;

public class CountMultiSearcher implements MultiSearcher {

	protected static final Logger LOG = LogManager.getLogger(CountMultiSearcher.class);

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
			throw new UnsupportedOperationException(
					"Sorted queries are not supported by SimpleUnsortedUnscoredLuceneMultiSearcher");
		}
		if (queryParams.needsScores() && queryParams.limitLong() > 0) {
			throw new UnsupportedOperationException(
					"Scored queries are not supported by SimpleUnsortedUnscoredLuceneMultiSearcher");
		}

		var results = indexSearchers
				.llShards()
				.stream()
				.map(searcher -> this.collect(searcher,
						queryParams,
						keyFieldName,
						transformer,
						f -> filterer.apply(f).limit(0)
				))
				.toList();
		boolean exactTotalHitsCount = true;
		long totalHitsCountValue = 0;
		for (LuceneSearchResult result : results) {
			exactTotalHitsCount &= result.totalHitsCount().exact();
			totalHitsCountValue += result.totalHitsCount().value();
		}

		var totalHitsCount = new TotalHitsCount(totalHitsCountValue, exactTotalHitsCount);

		return new LuceneSearchResult(totalHitsCount, List.of());
	}

	@Override
	public LuceneSearchResult collect(LLIndexSearcher indexSearcher,
			LocalQueryParams queryParams,
			@Nullable String keyFieldName,
			GlobalQueryRewrite transformer,
			Function<Stream<LLKeyScore>, Stream<LLKeyScore>> filterer) {
		if (transformer != GlobalQueryRewrite.NO_REWRITE) {
			return LuceneUtils.rewrite(this, indexSearcher, queryParams, keyFieldName, transformer, filterer);
		}
		try {
			var is = indexSearcher.getIndexSearcher();
			is.setTimeout(new QueryTimeoutImpl(queryParams.timeout().toMillis()));
			var count = is.count(queryParams.query());
			return new LuceneSearchResult(TotalHitsCount.of(count, true), List.of());
		} catch (IOException e) {
			throw new DBException(e);
		}
	}

	@Override
	public String getName() {
		return "count";
	}
}
