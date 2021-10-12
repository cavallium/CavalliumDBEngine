package it.cavallium.dbengine.lucene.searcher;

import static it.cavallium.dbengine.lucene.searcher.PaginationInfo.ALLOW_UNSCORED_PAGINATION_MODE;

import it.cavallium.dbengine.lucene.collector.UnscoredCollector;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopScoreDocCollector;

class TopDocsSearcher {

	@SuppressWarnings({"unchecked", "rawtypes"})
	public static TopDocsCollector<ScoreDoc> getTopDocsCollector(Sort luceneSort,
			int limit,
			ScoreDoc after,
			int totalHitsThreshold,
			boolean allowPagination,
			boolean computeScores) {
		TopDocsCollector<ScoreDoc> collector;
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
				collector = (TopDocsCollector<ScoreDoc>) (TopDocsCollector) TopFieldCollector.create(luceneSort, limit, totalHitsThreshold);
			} else if (after instanceof FieldDoc afterFieldDoc) {
				collector = (TopDocsCollector<ScoreDoc>) (TopDocsCollector) TopFieldCollector.create(luceneSort, limit, afterFieldDoc, totalHitsThreshold);
			} else {
				throw new UnsupportedOperationException("GetTopDocs with \"luceneSort\" != null requires \"after\" to be a FieldDoc");
			}
		}
		return collector;
	}
}
