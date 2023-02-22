package it.cavallium.dbengine.tests;

import static it.cavallium.dbengine.lucene.searcher.GlobalQueryRewrite.NO_REWRITE;

import com.google.common.collect.Streams;
import it.cavallium.dbengine.client.query.current.data.TotalHitsCount;
import it.cavallium.dbengine.database.LLKeyScore;
import it.cavallium.dbengine.database.disk.LLIndexSearchers;
import it.cavallium.dbengine.lucene.LuceneCloseable;
import it.cavallium.dbengine.lucene.LuceneUtils;
import it.cavallium.dbengine.lucene.searcher.GlobalQueryRewrite;
import it.cavallium.dbengine.lucene.searcher.LocalQueryParams;
import it.cavallium.dbengine.lucene.searcher.LocalSearcher;
import it.cavallium.dbengine.lucene.searcher.LuceneSearchResult;
import it.cavallium.dbengine.lucene.searcher.MultiSearcher;
import it.cavallium.dbengine.utils.SimpleResource;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class UnsortedUnscoredSimpleMultiSearcher implements MultiSearcher {

	private static final Logger LOG = LogManager.getLogger(UnsortedUnscoredSimpleMultiSearcher.class);

	private final LocalSearcher localSearcher;

	public UnsortedUnscoredSimpleMultiSearcher(LocalSearcher localSearcher) {
		this.localSearcher = localSearcher;
	}

	@Override
	public LuceneSearchResult collectMulti(LLIndexSearchers indexSearchers,
			LocalQueryParams queryParams,
			String keyFieldName,
			GlobalQueryRewrite transformer) {
		if (transformer != NO_REWRITE) {
			return LuceneUtils.rewriteMulti(this, indexSearchers, queryParams, keyFieldName, transformer);
		}
		if (queryParams.isSorted() && queryParams.limitLong() > 0) {
			throw new UnsupportedOperationException(
					"Sorted queries are not supported" + " by SimpleUnsortedUnscoredLuceneMultiSearcher");
		}
		if (queryParams.needsScores() && queryParams.limitLong() > 0) {
			throw new UnsupportedOperationException(
					"Scored queries are not supported" + " by SimpleUnsortedUnscoredLuceneMultiSearcher");
		}

		var localQueryParams = getLocalQueryParams(queryParams);
		var results = indexSearchers.llShards().stream()
				.map(searcher -> localSearcher.collect(searcher, localQueryParams, keyFieldName, transformer))
				.toList();
		List<LuceneSearchResult> resultsToDrop = new ArrayList<>(results.size());
		List<Stream<LLKeyScore>> resultsFluxes = new ArrayList<>(results.size());
		boolean exactTotalHitsCount = true;
		long totalHitsCountValue = 0;
		for (LuceneSearchResult result : results) {
			resultsToDrop.add(result);
			resultsFluxes.add(result.results());
			exactTotalHitsCount &= result.totalHitsCount().exact();
			totalHitsCountValue += result.totalHitsCount().value();
		}

		var totalHitsCount = new TotalHitsCount(totalHitsCountValue, exactTotalHitsCount);
		//noinspection unchecked
		Stream<LLKeyScore> mergedFluxes = (Stream<LLKeyScore>) (Stream) Streams.concat(resultsFluxes.toArray(Stream<?>[]::new))
				.skip(queryParams.offsetLong())
				.limit(queryParams.limitLong());

		return new MyLuceneSearchResult(totalHitsCount, mergedFluxes, resultsToDrop, indexSearchers);
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

	private static class MyLuceneSearchResult extends LuceneSearchResult implements LuceneCloseable {

		private final List<LuceneSearchResult> resultsToDrop;
		private final LLIndexSearchers indexSearchers;

		public MyLuceneSearchResult(TotalHitsCount totalHitsCount,
				Stream<LLKeyScore> mergedFluxes,
				List<LuceneSearchResult> resultsToDrop,
				LLIndexSearchers indexSearchers) {
			super(totalHitsCount, mergedFluxes);
			this.resultsToDrop = resultsToDrop;
			this.indexSearchers = indexSearchers;
		}

		@Override
		protected void onClose() {
			resultsToDrop.forEach(SimpleResource::close);
			try {
				indexSearchers.close();
			} catch (UncheckedIOException e) {
				LOG.error("Can't close index searchers", e);
			}
			super.onClose();
		}
	}
}
