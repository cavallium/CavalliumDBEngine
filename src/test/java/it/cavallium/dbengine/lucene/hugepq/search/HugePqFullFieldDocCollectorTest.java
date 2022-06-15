package it.cavallium.dbengine.lucene.hugepq.search;

import static org.junit.jupiter.api.Assertions.assertEquals;

import it.cavallium.dbengine.client.query.current.data.TotalHitsCount;
import it.cavallium.dbengine.database.disk.LLTempHugePqEnv;
import it.cavallium.dbengine.lucene.LLFieldDoc;
import it.cavallium.dbengine.lucene.LLScoreDoc;
import it.cavallium.dbengine.lucene.analyzer.WordAnalyzer;
import it.cavallium.dbengine.lucene.searcher.ShardIndexSearcher;
import it.cavallium.dbengine.lucene.searcher.SharedShardStatistics;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.TotalHits.Relation;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.util.QueryBuilder;
import org.junit.jupiter.api.Test;

public class HugePqFullFieldDocCollectorTest {
	Sort sort = new Sort(new SortedNumericSortField("number_sort", Type.LONG));
	Query luceneQuery = LongPoint.newRangeQuery("number", -100, 100);
	
	@Test
	public void testSingleShard() throws IOException {
		try (var dir = new ByteBuffersDirectory(); var env = new LLTempHugePqEnv()) {
			var analyzer = new WordAnalyzer(true, true);
			var writer = new IndexWriter(dir, new IndexWriterConfig(analyzer));
			writer.updateDocument(new Term("id", "00"), List.of(new SortedNumericDocValuesField("number_sort", 1), new LongPoint("number", 1)));
			writer.updateDocument(new Term("id", "01"), List.of(new SortedNumericDocValuesField("number_sort", 44), new LongPoint("number", 44)));
			writer.updateDocument(new Term("id", "02"), List.of(new SortedNumericDocValuesField("number_sort", 203), new LongPoint("number", 203)));
			writer.updateDocument(new Term("id", "03"), List.of(new SortedNumericDocValuesField("number_sort", 209), new LongPoint("number", 209)));
			writer.updateDocument(new Term("id", "04"), List.of(new SortedNumericDocValuesField("number_sort", -33), new LongPoint("number", -33)));
			writer.updateDocument(new Term("id", "05"), List.of(new SortedNumericDocValuesField("number_sort", 0), new LongPoint("number", 0)));
			writer.updateDocument(new Term("id", "06"), List.of(new SortedNumericDocValuesField("number_sort", 933), new LongPoint("number", 933)));
			writer.updateDocument(new Term("id", "07"), List.of(new SortedNumericDocValuesField("number_sort", 6), new LongPoint("number", 6)));
			writer.updateDocument(new Term("id", "08"), List.of(new SortedNumericDocValuesField("number_sort", -11), new LongPoint("number", -11)));
			writer.updateDocument(new Term("id", "09"), List.of(new SortedNumericDocValuesField("number_sort", 9996), new LongPoint("number", 9996)));
			writer.updateDocument(new Term("id", "10"), List.of(new SortedNumericDocValuesField("number_sort", 9), new LongPoint("number", 9)));
			writer.updateDocument(new Term("id", "11"), List.of(new SortedNumericDocValuesField("number_sort", 66), new LongPoint("number", 66)));
			writer.updateDocument(new Term("id", "12"), List.of(new SortedNumericDocValuesField("number_sort", 88), new LongPoint("number", 88)));
			writer.updateDocument(new Term("id", "13"), List.of(new SortedNumericDocValuesField("number_sort", 222), new LongPoint("number", 222)));
			writer.updateDocument(new Term("id", "14"), List.of(new SortedNumericDocValuesField("number_sort", -2), new LongPoint("number", -2)));
			writer.updateDocument(new Term("id", "15"), List.of(new SortedNumericDocValuesField("number_sort", 7), new LongPoint("number", 7)));
			writer.updateDocument(new Term("id", "16"), List.of(new SortedNumericDocValuesField("number_sort", 1010912093), new LongPoint("number", 1010912093)));
			writer.updateDocument(new Term("id", "17"), List.of(new SortedNumericDocValuesField("number_sort", -3894789), new LongPoint("number", -3894789)));
			writer.updateDocument(new Term("id", "18"), List.of(new SortedNumericDocValuesField("number_sort", 122), new LongPoint("number", 122)));
			writer.updateDocument(new Term("id", "19"), List.of(new SortedNumericDocValuesField("number_sort", 2), new LongPoint("number", 2)));
			writer.flush();
			writer.commit();
			try (var reader = DirectoryReader.open(writer, true, true)) {
				var searcher = new IndexSearcher(reader);
				var expectedResults = searcher.search(luceneQuery, 20, sort, false);
				var expectedTotalHits = new TotalHitsCount(expectedResults.totalHits.value, expectedResults.totalHits.relation == Relation.EQUAL_TO);
				var expectedDocs = Arrays
						.stream(expectedResults.scoreDocs)
						.map(sd -> (FieldDoc) sd)
						.map(fieldDoc -> new LLFieldDoc(fieldDoc.doc, fieldDoc.score, fieldDoc.shardIndex, Arrays.asList(fieldDoc.fields)))
						.toList();
				try (var collector = HugePqFullFieldDocCollector.create(env, sort, 20, Integer.MAX_VALUE)) {
					searcher.search(luceneQuery, collector);
					var docs = collector.fullDocs().iterate().collectList().blockOptional().orElseThrow();
					System.out.println("Expected docs:");
					for (var expectedDoc : expectedDocs) {
						System.out.println(expectedDoc);
					}
					System.out.println("");
					System.out.println("Obtained docs:");
					for (var doc : docs) {
						System.out.println(doc);
					}
					assertEquals(expectedDocs,
							docs.stream().map(elem -> new LLFieldDoc(elem.doc(), elem.score(), -1, elem.fields())).toList()
					);
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
			writer1.updateDocument(new Term("id", "00"), List.of(new SortedNumericDocValuesField("number_sort", 1), new LongPoint("number", 1)));
			writer1.updateDocument(new Term("id", "01"), List.of(new SortedNumericDocValuesField("number_sort", 44), new LongPoint("number", 44)));
			writer1.updateDocument(new Term("id", "02"), List.of(new SortedNumericDocValuesField("number_sort", 203), new LongPoint("number", 203)));
			writer1.updateDocument(new Term("id", "03"), List.of(new SortedNumericDocValuesField("number_sort", 209), new LongPoint("number", 209)));
			writer1.updateDocument(new Term("id", "04"), List.of(new SortedNumericDocValuesField("number_sort", -33), new LongPoint("number", -33)));
			writer1.updateDocument(new Term("id", "05"), List.of(new SortedNumericDocValuesField("number_sort", 0), new LongPoint("number", 0)));
			writer1.updateDocument(new Term("id", "06"), List.of(new SortedNumericDocValuesField("number_sort", 933), new LongPoint("number", 933)));
			writer1.updateDocument(new Term("id", "07"), List.of(new SortedNumericDocValuesField("number_sort", 6), new LongPoint("number", 6)));
			writer1.updateDocument(new Term("id", "08"), List.of(new SortedNumericDocValuesField("number_sort", -11), new LongPoint("number", -11)));
			writer1.updateDocument(new Term("id", "09"), List.of(new SortedNumericDocValuesField("number_sort", 9996), new LongPoint("number", 9996)));
			writer2.updateDocument(new Term("id", "10"), List.of(new SortedNumericDocValuesField("number_sort", 9), new LongPoint("number", 9)));
			writer2.updateDocument(new Term("id", "11"), List.of(new SortedNumericDocValuesField("number_sort", 66), new LongPoint("number", 66)));
			writer2.updateDocument(new Term("id", "12"), List.of(new SortedNumericDocValuesField("number_sort", 88), new LongPoint("number", 88)));
			writer2.updateDocument(new Term("id", "13"), List.of(new SortedNumericDocValuesField("number_sort", 222), new LongPoint("number", 222)));
			writer2.updateDocument(new Term("id", "14"), List.of(new SortedNumericDocValuesField("number_sort", -2), new LongPoint("number", -2)));
			writer2.updateDocument(new Term("id", "15"), List.of(new SortedNumericDocValuesField("number_sort", 7), new LongPoint("number", 7)));
			writer2.updateDocument(new Term("id", "16"), List.of(new SortedNumericDocValuesField("number_sort", 1010912093), new LongPoint("number", 1010912093)));
			writer2.updateDocument(new Term("id", "17"), List.of(new SortedNumericDocValuesField("number_sort", -3894789), new LongPoint("number", -3894789)));
			writer2.updateDocument(new Term("id", "18"), List.of(new SortedNumericDocValuesField("number_sort", 122), new LongPoint("number", 122)));
			writer2.updateDocument(new Term("id", "19"), List.of(new SortedNumericDocValuesField("number_sort", 2), new LongPoint("number", 2)));
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
				var standardSharedManager = TopFieldCollector.createSharedManager(sort, 20, null, Integer.MAX_VALUE);
				var standardCollector1 = standardSharedManager.newCollector();
				var standardCollector2 = standardSharedManager.newCollector();
				shardSearcher1.search(luceneQuery, standardCollector1);
				shardSearcher2.search(luceneQuery, standardCollector2);
				var expectedResults = standardSharedManager.reduce(List.of(standardCollector1, standardCollector2));
				var expectedTotalHits = new TotalHitsCount(expectedResults.totalHits.value, expectedResults.totalHits.relation == Relation.EQUAL_TO);
				var expectedDocs = Arrays
						.stream(expectedResults.scoreDocs)
						.map(sd -> (FieldDoc) sd)
						.map(fieldDoc -> new LLFieldDoc(fieldDoc.doc, fieldDoc.score, fieldDoc.shardIndex, Arrays.asList(fieldDoc.fields)))
						.toList();
				var collectorManager = HugePqFullFieldDocCollector.createSharedManager(env, sort, 20, Integer.MAX_VALUE);
				var collector1 = collectorManager.newCollector();
				var collector2 = collectorManager.newCollector();
				shardSearcher1.search(luceneQuery, collector1);
				shardSearcher2.search(luceneQuery, collector2);
				try (var results = collectorManager.reduce(List.of(collector1, collector2))) {
					var docs = results.iterate().collectList().blockOptional().orElseThrow();
					System.out.println("Expected docs:");
					for (var expectedDoc : expectedDocs) {
						System.out.println(expectedDoc);
					}
					System.out.println("");
					System.out.println("Obtained docs:");
					for (var doc : docs) {
						System.out.println(doc);
					}
					assertEquals(expectedDocs,
							docs.stream().map(elem -> new LLFieldDoc(elem.doc(), elem.score(), -1, elem.fields())).toList()
					);
					assertEquals(expectedTotalHits, new TotalHitsCount(results.totalHits().value, results.totalHits().relation == Relation.EQUAL_TO));
				}
			}
		}
	}
}