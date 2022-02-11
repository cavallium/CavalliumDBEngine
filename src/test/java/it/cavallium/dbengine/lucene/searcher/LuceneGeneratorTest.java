package it.cavallium.dbengine.lucene.searcher;

import it.cavallium.dbengine.lucene.ExponentialPageLimits;
import it.unimi.dsi.fastutil.longs.LongList;
import java.io.IOException;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.CustomHitsThresholdChecker;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class LuceneGeneratorTest {

	private static IndexSearcher is;
	private static ExponentialPageLimits pageLimits;
	private static IndexWriter iw;

	@BeforeAll
	public static void beforeAll() throws IOException {
		ByteBuffersDirectory dir = new ByteBuffersDirectory();
		Analyzer analyzer = new StandardAnalyzer();
		iw = new IndexWriter(dir, new IndexWriterConfig(analyzer));
		iw.addDocument(List.of(new LongPoint("position", 1, 1)));
		iw.addDocument(List.of(new LongPoint("position", 2, 3)));
		iw.addDocument(List.of(new LongPoint("position", 4, -1)));
		iw.addDocument(List.of(new LongPoint("position", 3, -54)));
		// Exactly 4 dummies
		iw.addDocument(List.<IndexableField>of(new StringField("dummy", "dummy", Store.NO)));
		iw.addDocument(List.<IndexableField>of(new StringField("dummy", "dummy", Store.NO)));
		iw.addDocument(List.<IndexableField>of(new StringField("dummy", "dummy", Store.NO)));
		iw.addDocument(List.<IndexableField>of(new StringField("dummy", "dummy", Store.NO)));
		// texts
		iw.addDocument(List.<IndexableField>of(new TextField("text", "prova abc", Store.YES)));
		iw.addDocument(List.<IndexableField>of(new TextField("text", "prova mario", Store.YES)));
		iw.addDocument(List.<IndexableField>of(new TextField("text", "luigi provi def", Store.YES)));
		iw.addDocument(List.<IndexableField>of(new TextField("text", "abc provo prova", Store.YES)));
		iw.addDocument(List.<IndexableField>of(new TextField("text", "prova abd", Store.YES)));
		iw.addDocument(List.<IndexableField>of(new TextField("text", "la prova abc def", Store.YES)));
		iw.commit();

		DirectoryReader ir = DirectoryReader.open(iw);
		is = new IndexSearcher(ir);
		pageLimits = new ExponentialPageLimits();
	}

	@Test
	public void testPosition() throws IOException {
		var query = LongPoint.newRangeQuery("position",
				LongList.of(1, -1).toLongArray(),
				LongList.of(2, 3).toLongArray()
		);
		int limit = Integer.MAX_VALUE;
		var localQueryParams = new LocalQueryParams(query, 0, limit, pageLimits, Duration.ofDays(1));

		var expectedResults = fixResults(localQueryParams.isSorted(),
				localQueryParams.needsScores(), List.of(is.search(query, limit).scoreDocs));

		var reactiveGenerator = LuceneGenerator.reactive(is, localQueryParams, -1);
		var results = fixResults(localQueryParams.isSorted(),
				localQueryParams.needsScores(), reactiveGenerator.collectList().block());

		Assertions.assertNotEquals(0, results.size());

		Assertions.assertEquals(expectedResults, results);
	}

	@Test
	public void testTextSearch() throws IOException, ParseException {
		QueryParser queryParser = new QueryParser("text", iw.getAnalyzer());
		Query query = queryParser.parse("prova~10 abc~10");
		int limit = Integer.MAX_VALUE;
		var localQueryParams = new LocalQueryParams(query, 0, limit, pageLimits, Duration.ofDays(1));

		var expectedResults = fixResults(localQueryParams.isSorted(),
				localQueryParams.needsScores(), List.of(is.search(query, limit).scoreDocs));

		var reactiveGenerator = LuceneGenerator.reactive(is, localQueryParams, -1);
		var results = fixResults(localQueryParams.isSorted(), localQueryParams.needsScores(), reactiveGenerator.collectList().block());

		Assertions.assertNotEquals(0, results.size());

		Assertions.assertEquals(expectedResults, results);
	}

	@Test
	public void testLimit0() {
		var query = LongPoint.newRangeQuery("position",
				LongList.of(1, -1).toLongArray(),
				LongList.of(2, 3).toLongArray()
		);
		var limitThresholdChecker = CustomHitsThresholdChecker.create(0);
		var reactiveGenerator = LuceneGenerator.reactive(is,
				new LocalQueryParams(query,
						0L,
						0,
						pageLimits,
						null,
						null,
						Duration.ofDays(1)
				),
				-1
		);
		var results = reactiveGenerator.collectList().block();

		Assertions.assertNotNull(results);
		Assertions.assertEquals(0, results.size());
	}

	@Test
	public void testLimitRestrictingResults() {
		var query = new TermQuery(new Term("dummy", "dummy"));
		int limit = 3; // the number of dummies - 1
		var reactiveGenerator = LuceneGenerator.reactive(is,
				new LocalQueryParams(query,
						0L,
						limit,
						pageLimits,
						null,
						null,
						Duration.ofDays(1)
				),
				-1
		);
		var results = reactiveGenerator.collectList().block();

		Assertions.assertNotNull(results);
		Assertions.assertEquals(limit, results.size());
	}

	@Test
	public void testDummies() throws IOException {
		var query = new TermQuery(new Term("dummy", "dummy"));
		int limit = Integer.MAX_VALUE;
		var localQueryParams = new LocalQueryParams(query, 0, limit, pageLimits, null, true, Duration.ofDays(1));

		var expectedResults = fixResults(localQueryParams.isSorted(), localQueryParams.needsScores(),
				List.of(is.search(query, limit, Sort.INDEXORDER, false).scoreDocs));

		var reactiveGenerator = LuceneGenerator.reactive(is, localQueryParams, -1);
		var results = fixResults(localQueryParams.isSorted(),
				localQueryParams.needsScores(), reactiveGenerator.collectList().block());

		Assertions.assertEquals(4, results.size());
		Assertions.assertEquals(expectedResults, results);
	}

	private List<ScoreDoc> fixResults(boolean sorted, boolean needsScores, List<ScoreDoc> results) {
		Assertions.assertNotNull(results);
		var s = results.stream().map(scoreDoc -> needsScores ? new MyScoreDoc(scoreDoc) : new UnscoredScoreDoc(scoreDoc));

		if (!sorted) {
			s = s.sorted(Comparator.comparingInt(d -> d.doc));
		}
		return s.collect(Collectors.toList());
	}

	private static class MyScoreDoc extends ScoreDoc {

		public MyScoreDoc(ScoreDoc scoreDoc) {
			super(scoreDoc.doc, scoreDoc.score, scoreDoc.shardIndex);
		}

		@Override
		public boolean equals(Object obj) {
			if (obj == this) {
				return true;
			}
			if (obj instanceof ScoreDoc sd) {
				return Objects.equals(this.score, sd.score)
						&& Objects.equals(this.doc, sd.doc)
						&& this.shardIndex == sd.shardIndex;
			}
			return false;
		}
	}

	private static class UnscoredScoreDoc extends ScoreDoc {

		public UnscoredScoreDoc(ScoreDoc scoreDoc) {
			super(scoreDoc.doc, scoreDoc.score, scoreDoc.shardIndex);
		}

		@Override
		public boolean equals(Object obj) {
			if (obj == this) {
				return true;
			}
			if (obj instanceof ScoreDoc sd) {
				return Objects.equals(this.doc, sd.doc)
						&& this.shardIndex == sd.shardIndex;
			}
			return false;
		}
	}

}
