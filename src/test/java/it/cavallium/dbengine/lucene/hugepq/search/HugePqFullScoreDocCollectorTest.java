package it.cavallium.dbengine.lucene.hugepq.search;

import static org.junit.jupiter.api.Assertions.*;

import it.cavallium.dbengine.client.query.QueryUtils;
import it.cavallium.dbengine.client.query.current.data.TotalHitsCount;
import it.cavallium.dbengine.database.disk.IndexSearcherManager;
import it.cavallium.dbengine.database.disk.LLTempHugePqEnv;
import it.cavallium.dbengine.lucene.LLScoreDoc;
import it.cavallium.dbengine.lucene.LuceneUtils;
import it.cavallium.dbengine.lucene.analyzer.LegacyWordAnalyzer;
import it.cavallium.dbengine.lucene.analyzer.TextFieldsAnalyzer;
import it.cavallium.dbengine.lucene.analyzer.WordAnalyzer;
import it.cavallium.dbengine.lucene.searcher.ShardIndexSearcher;
import it.cavallium.dbengine.lucene.searcher.SharedShardStatistics;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.TotalHits.Relation;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.util.QueryBuilder;
import org.junit.jupiter.api.Test;

public class HugePqFullScoreDocCollectorTest {

	@Test
	public void testSingleShard() throws IOException {
		try (var dir = new ByteBuffersDirectory(); var env = new LLTempHugePqEnv()) {
			var analyzer = new WordAnalyzer(true, true);
			var writer = new IndexWriter(dir, new IndexWriterConfig(analyzer));
			writer.updateDocument(new Term("id", "00"), List.of(new TextField("text", "Mario Rossi", Store.YES)));
			writer.updateDocument(new Term("id", "01"), List.of(new TextField("text", "Mario Rossi", Store.YES)));
			writer.updateDocument(new Term("id", "02"), List.of(new TextField("text", "Mario Rossi", Store.YES)));
			writer.updateDocument(new Term("id", "03"), List.of(new TextField("text", "Marios Rossi", Store.YES)));
			writer.updateDocument(new Term("id", "04"), List.of(new TextField("text", "Rossi", Store.YES)));
			writer.updateDocument(new Term("id", "05"), List.of(new TextField("text", "Rossi", Store.YES)));
			writer.updateDocument(new Term("id", "06"), List.of(new TextField("text", "Rossi", Store.YES)));
			writer.updateDocument(new Term("id", "07"), List.of(new TextField("text", "Rossi", Store.YES)));
			writer.updateDocument(new Term("id", "08"), List.of(new TextField("text", "Rossi", Store.YES)));
			writer.updateDocument(new Term("id", "09"), List.of(new TextField("text", "Rossi", Store.YES)));
			writer.updateDocument(new Term("id", "10"), List.of(new TextField("text", "ROSSI UA", Store.YES)));
			writer.updateDocument(new Term("id", "11"), List.of(new TextField("text", "Mario Barman", Store.YES)));
			writer.updateDocument(new Term("id", "12"), List.of(new TextField("text", "Mario batman", Store.YES)));
			writer.updateDocument(new Term("id", "13"), List.of(new TextField("text", "Admin Rossi desk", Store.YES)));
			writer.updateDocument(new Term("id", "14"), List.of(new TextField("text", "MRI Marios bot", Store.YES)));
			writer.updateDocument(new Term("id", "15"), List.of(new TextField("text", "Mario Rossi [beta]", Store.YES)));
			writer.updateDocument(new Term("id", "16"), List.of(new TextField("text", "Mario Music Bot", Store.YES)));
			writer.updateDocument(new Term("id", "17"), List.of(new TextField("text", "Mario night mode", Store.YES)));
			writer.updateDocument(new Term("id", "18"), List.of(new TextField("text", "Mario stats bot", Store.YES)));
			writer.updateDocument(new Term("id", "19"), List.of(new TextField("text", "Very very long text with Mario Giovanni and Rossi inside", Store.YES)));
			writer.flush();
			writer.commit();
			try (var reader = DirectoryReader.open(writer, true, true)) {
				var searcher = new IndexSearcher(reader);
				var qb = new QueryBuilder(analyzer);
				var luceneQuery = qb.createMinShouldMatchQuery("text", "Mario rossi", 0.3f);
				var expectedResults = searcher.search(luceneQuery, 20);
				var expectedTotalHits = new TotalHitsCount(expectedResults.totalHits.value, expectedResults.totalHits.relation == Relation.EQUAL_TO);
				var expectedDocs = Arrays
						.stream(expectedResults.scoreDocs)
						.map(scoreDoc -> new LLScoreDoc(scoreDoc.doc, scoreDoc.score, scoreDoc.shardIndex))
						.toList();
				try (var collector = HugePqFullScoreDocCollector.create(env, 20)) {
					searcher.search(luceneQuery, collector);
					var docs = collector.fullDocs().iterate().collectList().blockOptional().orElseThrow();
					System.out.println("Expected docs:");
					for (LLScoreDoc expectedDoc : expectedDocs) {
						System.out.println(expectedDoc);
					}
					System.out.println("");
					System.out.println("Obtained docs:");
					for (LLScoreDoc doc : docs) {
						System.out.println(doc);
					}
					assertEquals(expectedDocs, docs.stream().map(elem -> new LLScoreDoc(elem.doc(), elem.score(), -1)).toList());
					assertEquals(expectedTotalHits, new TotalHitsCount(collector.getTotalHits(), true));
				}
			}
		}
	}

	@Test
	public void testMultiShard() throws IOException {
		try (var dir1 = new ByteBuffersDirectory(); var dir2 = new ByteBuffersDirectory(); var env = new LLTempHugePqEnv()) {
			var analyzer = new WordAnalyzer(true, true);
			var writer1 = new IndexWriter(dir1, new IndexWriterConfig(analyzer));
			var writer2 = new IndexWriter(dir2, new IndexWriterConfig(analyzer));
			writer1.updateDocument(new Term("id", "00"), List.of(new TextField("text", "Mario Rossi", Store.YES)));
			writer1.updateDocument(new Term("id", "01"), List.of(new TextField("text", "Mario Rossi", Store.YES)));
			writer1.updateDocument(new Term("id", "02"), List.of(new TextField("text", "Mario Rossi", Store.YES)));
			writer1.updateDocument(new Term("id", "03"), List.of(new TextField("text", "Marios Rossi", Store.YES)));
			writer1.updateDocument(new Term("id", "04"), List.of(new TextField("text", "Rossi", Store.YES)));
			writer1.updateDocument(new Term("id", "05"), List.of(new TextField("text", "Rossi", Store.YES)));
			writer1.updateDocument(new Term("id", "06"), List.of(new TextField("text", "Rossi", Store.YES)));
			writer1.updateDocument(new Term("id", "07"), List.of(new TextField("text", "Rossi", Store.YES)));
			writer1.updateDocument(new Term("id", "08"), List.of(new TextField("text", "Rossi", Store.YES)));
			writer1.updateDocument(new Term("id", "09"), List.of(new TextField("text", "Rossi", Store.YES)));
			writer2.updateDocument(new Term("id", "10"), List.of(new TextField("text", "ROSSI UA", Store.YES)));
			writer2.updateDocument(new Term("id", "11"), List.of(new TextField("text", "Mario Barman", Store.YES)));
			writer2.updateDocument(new Term("id", "12"), List.of(new TextField("text", "Mario batman", Store.YES)));
			writer2.updateDocument(new Term("id", "13"), List.of(new TextField("text", "Admin Rossi desk", Store.YES)));
			writer2.updateDocument(new Term("id", "14"), List.of(new TextField("text", "MRI Marios bot", Store.YES)));
			writer2.updateDocument(new Term("id", "15"), List.of(new TextField("text", "Mario Rossi [beta]", Store.YES)));
			writer2.updateDocument(new Term("id", "16"), List.of(new TextField("text", "Mario Music Bot", Store.YES)));
			writer2.updateDocument(new Term("id", "17"), List.of(new TextField("text", "Mario night mode", Store.YES)));
			writer2.updateDocument(new Term("id", "18"), List.of(new TextField("text", "Mario stats bot", Store.YES)));
			writer2.updateDocument(new Term("id", "19"), List.of(new TextField("text", "Very very long text with Mario Giovanni and Rossi inside", Store.YES)));
			writer1.flush();
			writer2.flush();
			writer1.commit();
			writer2.commit();
			var sharedStats = new SharedShardStatistics();
			try (var reader1 = DirectoryReader.open(writer1, true, true);
					var reader2 = DirectoryReader.open(writer2, true, true)) {
				var searcher1 = new IndexSearcher(reader1);
				var searcher2 = new IndexSearcher(reader2);
				var shardSearcher1 = new ShardIndexSearcher(sharedStats, List.of(searcher1, searcher2), 0);
				var shardSearcher2 = new ShardIndexSearcher(sharedStats, List.of(searcher1, searcher2), 1);
				var qb = new QueryBuilder(analyzer);
				var luceneQuery = qb.createMinShouldMatchQuery("text", "Mario rossi", 0.3f);
				var standardSharedManager = TopScoreDocCollector.createSharedManager(20, null, Integer.MAX_VALUE);
				var standardCollector1 = standardSharedManager.newCollector();
				var standardCollector2 = standardSharedManager.newCollector();
				shardSearcher1.search(luceneQuery, standardCollector1);
				shardSearcher2.search(luceneQuery, standardCollector2);
				var expectedResults = standardSharedManager.reduce(List.of(standardCollector1, standardCollector2));
				var expectedTotalHits = new TotalHitsCount(expectedResults.totalHits.value, expectedResults.totalHits.relation == Relation.EQUAL_TO);
				var expectedDocs = Arrays
						.stream(expectedResults.scoreDocs)
						.map(scoreDoc -> new LLScoreDoc(scoreDoc.doc, scoreDoc.score, scoreDoc.shardIndex))
						.toList();
				var collectorManager = HugePqFullScoreDocCollector.createSharedManager(env, 20, Integer.MAX_VALUE);
				var collector1 = collectorManager.newCollector();
				var collector2 = collectorManager.newCollector();
				shardSearcher1.search(luceneQuery, collector1);
				shardSearcher2.search(luceneQuery, collector2);
				try (var results = collectorManager.reduce(List.of(collector1, collector2))) {
					var docs = results.iterate().collectList().blockOptional().orElseThrow();
					System.out.println("Expected docs:");
					for (LLScoreDoc expectedDoc : expectedDocs) {
						System.out.println(expectedDoc);
					}
					System.out.println("");
					System.out.println("Obtained docs:");
					for (LLScoreDoc doc : docs) {
						System.out.println(doc);
					}
					assertEquals(expectedDocs, docs.stream().map(elem -> new LLScoreDoc(elem.doc(), elem.score(), -1)).toList());
					assertEquals(expectedTotalHits, new TotalHitsCount(results.totalHits().value, results.totalHits().relation == Relation.EQUAL_TO));
				}
			}
		}
	}
}