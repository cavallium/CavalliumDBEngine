package it.cavallium.dbengine.lucene.collector;

import static it.cavallium.dbengine.lucene.searcher.PaginationInfo.ALLOW_UNSCORED_PAGINATION_MODE;

import it.cavallium.dbengine.lucene.LuceneUtils;
import it.cavallium.dbengine.lucene.collector.UnscoredCollector;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TopScoreDocCollector;
import org.jetbrains.annotations.NotNull;

public class TopDocsCollectorMultiManager implements CollectorMultiManager<TopDocs, TopDocs> {

	private final Sort luceneSort;
	private final int limit;
	private final ScoreDoc after;
	private final int totalHitsThreshold;
	private final boolean allowPagination;
	private final boolean computeScores;

	private final int topDocsOffset;
	private final int topDocsCount;

	public TopDocsCollectorMultiManager(Sort luceneSort,
			int limit,
			ScoreDoc after,
			int totalHitsThreshold,
			boolean allowPagination,
			boolean computeScores,
			int topDocsOffset,
			int topDocsCount) {
		this.luceneSort = luceneSort;
		this.limit = limit;
		this.after = after;
		this.totalHitsThreshold = totalHitsThreshold;
		this.allowPagination = allowPagination;
		this.computeScores = computeScores;

		this.topDocsOffset = topDocsOffset;
		this.topDocsCount = topDocsCount;
	}

	public CollectorManager<TopDocsCollector<?>, TopDocs> get(@NotNull Query query, IndexSearcher indexSearcher) {
		return new CollectorManager<>() {
			@Override
			public TopDocsCollector<?> newCollector() throws IOException {
				TopDocsCollector<?> collector;
				if (after != null && !allowPagination) {
					throw new IllegalArgumentException("\"allowPagination\" is false, but \"after\" is set");
				}
				if (luceneSort == null) {
					if (after == null) {
						if (computeScores || allowPagination || !ALLOW_UNSCORED_PAGINATION_MODE) {
							collector = TopScoreDocCollector.create(limit, totalHitsThreshold);
						} else {
							collector = new UnscoredCollector(limit);
						}
					} else {
						collector = TopScoreDocCollector.create(limit, after, totalHitsThreshold);
					}
				} else {
					if (after == null) {
						collector = TopFieldCollector.create(luceneSort, limit, totalHitsThreshold);
					} else if (after instanceof FieldDoc afterFieldDoc) {
						collector = TopFieldCollector.create(luceneSort, limit, afterFieldDoc, totalHitsThreshold);
					} else {
						throw new UnsupportedOperationException("GetTopDocs with \"luceneSort\" != null requires \"after\" to be a FieldDoc");
					}
				}
				return collector;
			}

			@Override
			public TopDocs reduce(Collection<TopDocsCollector<?>> collectors) throws IOException {
				TopDocs[] docsArray;
				boolean needsSort = luceneSort != null;
				boolean needsScores = luceneSort != null && luceneSort.needsScores();
				if (needsSort) {
					docsArray = new TopFieldDocs[collectors.size()];
				} else {
					docsArray = new TopDocs[collectors.size()];
				}
				int i = 0;
				for (TopDocsCollector<?> collector : collectors) {
					docsArray[i] = collector.topDocs();
					i++;
				}
				var merged = LuceneUtils.mergeTopDocs(luceneSort, null, null, docsArray);
				if (needsScores) {
					TopFieldCollector.populateScores(merged.scoreDocs, indexSearcher, query);
				}
				return merged;
			}
		};
	}

	@Override
	public ScoreMode scoreMode() {
		throw new NotImplementedException();
	}

	@SuppressWarnings({"SuspiciousToArrayCall", "IfStatementWithIdenticalBranches"})
	@Override
	public TopDocs reduce(List<TopDocs> topDocs) {
		TopDocs[] arr;
		if (luceneSort != null) {
			arr = topDocs.toArray(TopFieldDocs[]::new);
		} else {
			arr = topDocs.toArray(TopDocs[]::new);
		}
		return LuceneUtils.mergeTopDocs(luceneSort, topDocsOffset, topDocsCount, arr);
	}
}
