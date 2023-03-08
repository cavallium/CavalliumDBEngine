package it.cavallium.dbengine.lucene.searcher;

import static it.cavallium.dbengine.database.LLUtils.mapList;
import static it.cavallium.dbengine.utils.StreamUtils.toList;
import static java.util.Objects.requireNonNull;

import it.cavallium.dbengine.client.query.current.data.TotalHitsCount;
import it.cavallium.dbengine.database.LLKeyScore;
import it.cavallium.dbengine.database.disk.LLIndexSearchers;
import it.cavallium.dbengine.lucene.LuceneCloseable;
import it.cavallium.dbengine.lucene.LuceneUtils;
import it.cavallium.dbengine.utils.DBException;
import it.cavallium.dbengine.utils.StreamUtils;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TopScoreDocCollector;
import org.jetbrains.annotations.Nullable;

public class StandardSearcher implements MultiSearcher {

	protected static final Logger LOG = LogManager.getLogger(StandardSearcher.class);

	public StandardSearcher() {
	}

	@Override
	public LuceneSearchResult collectMulti(LLIndexSearchers indexSearchers,
			LocalQueryParams queryParams,
			@Nullable String keyFieldName,
			GlobalQueryRewrite transformer,
			Function<Stream<LLKeyScore>, Stream<LLKeyScore>> filterer) {
		if (transformer != GlobalQueryRewrite.NO_REWRITE) {
			return LuceneUtils.rewriteMulti(this, indexSearchers, queryParams, keyFieldName, transformer, filterer);
		}
		// Search results
		var fullDocs = this.search(indexSearchers.shards(), queryParams);
		// Compute the results
		return this.computeResults(fullDocs, indexSearchers, keyFieldName, queryParams, filterer);
	}

	/**
	 * Search effectively the raw results
	 */
	@SuppressWarnings({"rawtypes"})
	private TopDocs search(Collection<IndexSearcher> indexSearchers, LocalQueryParams queryParams) {
		var totalHitsThreshold = queryParams.getTotalHitsThresholdInt();
		CollectorManager<? extends TopDocsCollector<?>, ? extends TopDocs> sharedManager;
		if (queryParams.isSorted() && !queryParams.isSortedByScore()) {
			sharedManager = TopFieldCollector.createSharedManager(queryParams.sort(),
					queryParams.limitInt(), null, totalHitsThreshold
			);
		} else {
			sharedManager = TopScoreDocCollector.createSharedManager(queryParams.limitInt(), null, totalHitsThreshold);
		}
		;
		var collectors = mapList(indexSearchers, shard -> {
			try {
				TopDocsCollector<?> collector;
				collector = sharedManager.newCollector();
				assert queryParams.computePreciseHitsCount() == null || (queryParams.computePreciseHitsCount() == collector
						.scoreMode()
						.isExhaustive());

				shard.search(queryParams.query(), LuceneUtils.withTimeout(collector, queryParams.timeout()));
				return collector;
			} catch (IOException e) {
				throw new DBException(e);
			}
		});

		try {
			if (collectors.size() <= 1) {
				//noinspection unchecked
				return sharedManager.reduce((List) collectors);
			} else if (queryParams.isSorted() && !queryParams.isSortedByScore()) {
				final TopFieldDocs[] topDocs = new TopFieldDocs[collectors.size()];
				int i = 0;
				for (var collector : collectors) {
					var topFieldDocs = ((TopFieldCollector) collector).topDocs();
					for (ScoreDoc scoreDoc : topFieldDocs.scoreDocs) {
						scoreDoc.shardIndex = i;
					}
					topDocs[i++] = topFieldDocs;
				}
				return TopDocs.merge(requireNonNull(queryParams.sort()), 0, queryParams.limitInt(), topDocs);
			} else {
				final TopDocs[] topDocs = new TopDocs[collectors.size()];
				int i = 0;
				for (var collector : collectors) {
					var topScoreDocs = collector.topDocs();
					for (ScoreDoc scoreDoc : topScoreDocs.scoreDocs) {
						scoreDoc.shardIndex = i;
					}
					topDocs[i++] = topScoreDocs;
				}
				return TopDocs.merge(0, queryParams.limitInt(), topDocs);
			}
		} catch (IOException ex) {
			throw new DBException(ex);
		}
	}

	/**
	 * Compute the results, extracting useful data
	 */
	private LuceneSearchResult computeResults(TopDocs data,
			LLIndexSearchers indexSearchers,
			String keyFieldName,
			LocalQueryParams queryParams,
			Function<Stream<LLKeyScore>, Stream<LLKeyScore>> filterer) {
		var totalHitsCount = LuceneUtils.convertTotalHitsCount(data.totalHits);

		Stream<LLKeyScore> hitsStream = LuceneUtils
				.convertHits(Stream.of(data.scoreDocs), indexSearchers.shards(), keyFieldName)
				.skip(queryParams.offsetLong())
				.limit(queryParams.limitLong());

		return new LuceneSearchResult(totalHitsCount, toList(filterer.apply(hitsStream)));
	}

	@Override
	public String getName() {
		return "standard";
	}
}
