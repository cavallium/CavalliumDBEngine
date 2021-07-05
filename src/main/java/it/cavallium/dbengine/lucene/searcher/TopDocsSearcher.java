package it.cavallium.dbengine.lucene.searcher;

import java.io.IOException;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiCollectorManager.Collectors;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopScoreDocCollector;

class TopDocsSearcher {

	private final boolean doDocScores;
	private final IndexSearcher indexSearcher;
	private final Query query;
	private final Sort luceneSort;
	private final int limit;
	private final FieldDoc after;
	private final int totalHitsThreshold;

	@Deprecated
	public TopDocsSearcher(IndexSearcher indexSearcher,
			Query query,
			Sort luceneSort,
			int limit,
			FieldDoc after,
			boolean doDocScores,
			int totalHitsThreshold) {
		this.indexSearcher = indexSearcher;
		this.query = query;
		this.luceneSort = luceneSort;
		this.limit = limit;
		this.after = after;
		this.doDocScores = doDocScores;
		this.totalHitsThreshold = totalHitsThreshold;
	}

	/**
	 * This method must not be called more than once!
	 */
	@Deprecated
	public TopDocs getTopDocs(int offset, int limit) throws IOException {
		return getTopDocs(indexSearcher, query, luceneSort, limit, after, doDocScores, totalHitsThreshold, offset, limit);
	}

	/**
	 * This method must not be called more than once!
	 */
	@Deprecated
	public TopDocs getTopDocs() throws IOException {
		return getTopDocs(indexSearcher, query, luceneSort, limit, after, doDocScores, totalHitsThreshold);
	}

	public static TopDocs getTopDocs(IndexSearcher indexSearcher,
			Query query,
			Sort luceneSort,
			int limit,
			ScoreDoc after,
			boolean doDocScores,
			int totalHitsThreshold,

			int topDocsStartOffset,
			int topDocsHowMany) throws IOException {
		TopDocsCollector<?> collector = getTopDocsCollector(luceneSort, limit, after, totalHitsThreshold);
		indexSearcher.search(query, collector);
		TopDocs topDocs = collector.topDocs(topDocsStartOffset, topDocsHowMany);
		if (doDocScores) {
			TopFieldCollector.populateScores(topDocs.scoreDocs, indexSearcher, query);
		}
		return topDocs;
	}

	public static TopDocs getTopDocs(IndexSearcher indexSearcher,
			Query query,
			Sort luceneSort,
			int limit,
			ScoreDoc after,
			boolean doDocScores,
			int totalHitsThreshold) throws IOException {
		TopDocsCollector<?> collector = getTopDocsCollector(luceneSort, limit, after, totalHitsThreshold);
		indexSearcher.search(query, collector);
		TopDocs topDocs = collector.topDocs();
		if (doDocScores) {
			TopFieldCollector.populateScores(topDocs.scoreDocs, indexSearcher, query);
		}
		return topDocs;
	}

	public static TopDocsCollector<?> getTopDocsCollector(Sort luceneSort,
			int limit,
			ScoreDoc after,
			int totalHitsThreshold) {
		TopDocsCollector<?> collector;
		if (luceneSort == null) {
			if (after == null) {
				collector = TopScoreDocCollector.create(limit, totalHitsThreshold);
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
}
