package it.cavallium.dbengine.lucene.searcher;

import java.io.IOException;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopScoreDocCollector;

class TopDocsSearcher {

	private final TopDocsCollector<?> collector;
	private final boolean doDocScores;
	private final IndexSearcher indexSearcher;
	private final Query query;

	public TopDocsSearcher(IndexSearcher indexSearcher,
			Query query,
			Sort luceneSort,
			int limit,
			FieldDoc after,
			boolean doDocScores,
			int totalHitsThreshold) throws IOException {
		if (luceneSort == null) {
			this.collector = TopScoreDocCollector.create(limit, after, totalHitsThreshold);
		} else {
			this.collector = TopFieldCollector.create(luceneSort, limit, after, totalHitsThreshold);
		}
		this.indexSearcher = indexSearcher;
		this.query = query;
		this.doDocScores = doDocScores;
		indexSearcher.search(query, collector);
	}

	public TopDocs getTopDocs(int offset, int length) throws IOException {
		TopDocs topDocs = collector.topDocs(offset, length);
		if (doDocScores) {
			TopFieldCollector.populateScores(topDocs.scoreDocs, indexSearcher, query);
		}
		return topDocs;
	}
}
