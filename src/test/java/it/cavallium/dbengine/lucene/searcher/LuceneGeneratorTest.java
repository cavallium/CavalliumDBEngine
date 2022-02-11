package it.cavallium.dbengine.lucene.searcher;

import it.cavallium.dbengine.lucene.ExponentialPageLimits;
import it.cavallium.dbengine.lucene.MaxScoreAccumulator;
import it.unimi.dsi.fastutil.longs.LongList;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.CustomHitsThresholdChecker;
import org.apache.lucene.search.IndexSearcher;
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

	@BeforeAll
	public static void beforeAll() throws IOException {
		ByteBuffersDirectory dir = new ByteBuffersDirectory();
		IndexWriter iw = new IndexWriter(dir, new IndexWriterConfig());
		iw.addDocument(List.of(new LongPoint("position", 1, 1)));
		iw.addDocument(List.of(new LongPoint("position", 2, 3)));
		iw.addDocument(List.of(new LongPoint("position", 4, -1)));
		iw.addDocument(List.of(new LongPoint("position", 3, -54)));
		// Exactly 4 dummies
		iw.addDocument(List.<IndexableField>of(new StringField("dummy", "dummy", Store.NO)));
		iw.addDocument(List.<IndexableField>of(new StringField("dummy", "dummy", Store.NO)));
		iw.addDocument(List.<IndexableField>of(new StringField("dummy", "dummy", Store.NO)));
		iw.addDocument(List.<IndexableField>of(new StringField("dummy", "dummy", Store.NO)));
		iw.commit();

		DirectoryReader ir = DirectoryReader.open(iw);
		is = new IndexSearcher(ir);
		pageLimits = new ExponentialPageLimits();
	}

	@Test
	public void test() throws IOException {
		var query = LongPoint.newRangeQuery("position",
				LongList.of(1, -1).toLongArray(),
				LongList.of(2, 3).toLongArray()
		);
		int limit = Integer.MAX_VALUE;
		var limitThresholdChecker = CustomHitsThresholdChecker.create(limit);

		var expectedResults = fixResults(fixResults(List.of(is.search(query, limit, Sort.RELEVANCE, true).scoreDocs)));

		var reactiveGenerator = LuceneGenerator.reactive(is,
				new LocalQueryParams(query,
						0L,
						limit,
						pageLimits,
						null,
						true,
						Duration.ofDays(1)
				),
				-1,
				limitThresholdChecker
		);
		var results = fixResults(reactiveGenerator.collectList().block());

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
						Sort.RELEVANCE,
						true,
						Duration.ofDays(1)
				),
				-1,
				limitThresholdChecker
		);
		var results = reactiveGenerator.collectList().block();

		Assertions.assertNotNull(results);
		Assertions.assertEquals(0, results.size());
	}

	@Test
	public void testLimitRestrictingResults() {
		var query = new TermQuery(new Term("dummy", "dummy"));
		int limit = 3; // the number of dummies - 1
		var limitThresholdChecker = CustomHitsThresholdChecker.create(limit);
		var reactiveGenerator = LuceneGenerator.reactive(is,
				new LocalQueryParams(query,
						0L,
						limit,
						pageLimits,
						null,
						true,
						Duration.ofDays(1)
				),
				-1,
				limitThresholdChecker
		);
		var results = reactiveGenerator.collectList().block();

		Assertions.assertNotNull(results);
		Assertions.assertEquals(limit, results.size());
	}

	@Test
	public void testDummies() throws IOException {
		var query = new TermQuery(new Term("dummy", "dummy"));
		int limit = Integer.MAX_VALUE;

		var expectedResults = fixResults(List.of(is.search(query, limit).scoreDocs));

		var limitThresholdChecker = CustomHitsThresholdChecker.create(limit);
		var reactiveGenerator = LuceneGenerator.reactive(is,
				new LocalQueryParams(query,
						0,
						limit,
						pageLimits,
						null,
						true,
						Duration.ofDays(1)
				),
				-1,
				limitThresholdChecker
		);
		var results = fixResults(reactiveGenerator.collectList().block());

		Assertions.assertEquals(4, results.size());
		Assertions.assertEquals(expectedResults, fixResults(results));
	}

	private List<ScoreDoc> fixResults(List<ScoreDoc> results) {
		Assertions.assertNotNull(results);
		return results.stream().map(MyScoreDoc::new).collect(Collectors.toList());
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
				return this.score == sd.score && this.doc == sd.doc && this.shardIndex == sd.shardIndex;
			}
			return false;
		}
	}

}
