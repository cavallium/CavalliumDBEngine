package it.cavallium.dbengine.tests;

import static it.cavallium.dbengine.tests.DbTestUtils.MAX_IN_MEMORY_RESULT_ENTRIES;
import static it.cavallium.dbengine.tests.DbTestUtils.ensureNoLeaks;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import it.cavallium.dbengine.tests.DbTestUtils.TempDb;
import it.cavallium.buffer.Buf;
import it.cavallium.dbengine.client.LuceneIndex;
import it.cavallium.dbengine.client.Sort;
import it.cavallium.dbengine.client.query.current.data.MatchAllDocsQuery;
import it.cavallium.dbengine.database.LLLuceneIndex;
import it.cavallium.dbengine.database.LLScoreMode;
import it.cavallium.dbengine.lucene.searcher.AdaptiveLocalSearcher;
import it.cavallium.dbengine.lucene.searcher.AdaptiveMultiSearcher;
import it.cavallium.dbengine.lucene.searcher.CountMultiSearcher;
import it.cavallium.dbengine.lucene.searcher.LocalSearcher;
import it.cavallium.dbengine.lucene.searcher.MultiSearcher;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestLuceneIndex {

	private final Logger log = LogManager.getLogger(this.getClass());
	private TempDb tempDb;
	private LLLuceneIndex luceneSingle;
	private LLLuceneIndex luceneMulti;

	protected TemporaryDbGenerator getTempDbGenerator() {
		return new MemoryTemporaryDbGenerator();
	}

	@BeforeAll
	public static void beforeAll() throws IOException {
	}

	@BeforeEach
	public void beforeEach() throws IOException {
		ensureNoLeaks();
		tempDb = Objects.requireNonNull(getTempDbGenerator().openTempDb(), "TempDB");
		luceneSingle = tempDb.luceneSingle();
		luceneMulti = tempDb.luceneMulti();
	}

	public static Stream<Arguments> provideArguments() {
		return Stream.of(false, true).map(Arguments::of);
	}

	private static final List<Boolean> multi = List.of(false, true);
	private static final List<LLScoreMode> scoreModes = List.of(LLScoreMode.NO_SCORES,
			LLScoreMode.TOP_SCORES,
			LLScoreMode.COMPLETE_NO_SCORES,
			LLScoreMode.COMPLETE
	);
	private static final List<Sort> multiSort = List.of(Sort.score(),
			Sort.random(),
			Sort.no(),
			Sort.doc(),
			Sort.numeric("longsort", false),
			Sort.numeric("longsort", true)
	);

	record Tuple2<X, Y>(X getT1, Y getT2) {

		public Object[] toArray() {
			return new Object[] {getT1, getT2};
		}
	}
	record Tuple3<X, Y, Z>(X getT1, Y getT2, Z getT3) {

		public Object[] toArray() {
			return new Object[] {getT1, getT2, getT3};
		}
	}
	record Tuple4<X, Y, Z, W>(X getT1, Y getT2, Z getT3, W getT4) {

		public Object[] toArray() {
			return new Object[] {getT1, getT2, getT3, getT4};
		}
	}
	record Tuple5<X, Y, Z, W, X1>(X getT1, Y getT2, Z getT3, W getT4, X1 getT5) {

		public Object[] toArray() {
			return new Object[] {getT1, getT2, getT3, getT4, getT5};
		}
	}

	public static Stream<Arguments> provideQueryArgumentsScoreMode() {
		return multi.stream()
				.flatMap(shard -> scoreModes.stream().map(scoreMode -> new Tuple2<>(shard, scoreMode)))
				.map(tuple -> Arguments.of(tuple.toArray()));
	}

	public static Stream<Arguments> provideQueryArgumentsSort() {
		return multi.stream()
				.flatMap(shard -> multiSort.stream().map(multiSort -> new Tuple2<>(shard, multiSort)))
				.map(tuple -> Arguments.of(tuple.toArray()));
	}

	public static Stream<Arguments> provideQueryArgumentsScoreModeAndSort() {
		return multi.stream()
				.flatMap(shard -> scoreModes.stream().map(scoreMode -> new Tuple2<>(shard, scoreMode)))
				.flatMap(tuple -> multiSort.stream().map(multiSort -> new Tuple3<>(tuple.getT1(), tuple.getT2(), multiSort)))
				.map(tuple -> Arguments.of(tuple.toArray()));
	}

	@AfterEach
	public void afterEach() throws IOException {
		getTempDbGenerator().closeTempDb(tempDb);
		ensureNoLeaks();
	}

	@AfterAll
	public static void afterAll() throws IOException {
	}

	private LuceneIndex<String, String> getLuceneIndex(boolean shards, @Nullable LocalSearcher customSearcher) {
		LuceneIndex<String, String> index = DbTestUtils.tempLuceneIndex(shards ? luceneSingle : luceneMulti);
		index.updateDocument("test-key-1", "0123456789");
		index.updateDocument("test-key-2", "test 0123456789 test word");
		index.updateDocument("test-key-3", "0123456789 test example string");
		index.updateDocument("test-key-4", "hello world the quick brown fox jumps over the lazy dog");
		index.updateDocument("test-key-5", "hello the quick brown fox jumps over the lazy dog");
		index.updateDocument("test-key-6", "hello the quick brown fox jumps over the world dog");
		index.updateDocument("test-key-7", "the quick brown fox jumps over the world dog");
		index.updateDocument("test-key-8", "the quick brown fox jumps over the lazy dog");
		index.updateDocument("test-key-9", "Example1");
		index.updateDocument("test-key-10", "Example2");
		index.updateDocument("test-key-11", "Example3");
		index.updateDocument("test-key-12", "-234");
		index.updateDocument("test-key-13", "2111");
		index.updateDocument("test-key-14", "2999");
		index.updateDocument("test-key-15", "3902");
		IntStream.rangeClosed(1, 1000).forEach(i -> index.updateDocument("test-key-" + (15 + i), "" + i));
		tempDb.swappableLuceneSearcher().setSingle(new CountMultiSearcher());
		tempDb.swappableLuceneSearcher().setMulti(new CountMultiSearcher());
		assertCount(index, 1000 + 15);
		if (customSearcher != null) {
			tempDb.swappableLuceneSearcher().setSingle(customSearcher);
			if (shards) {
				if (customSearcher instanceof MultiSearcher multiSearcher) {
					tempDb.swappableLuceneSearcher().setMulti(multiSearcher);
				} else {
					throw new IllegalArgumentException("Expected a LuceneMultiSearcher, got a LuceneLocalSearcher: " + customSearcher.toString());
				}
			}
		} else {
			tempDb.swappableLuceneSearcher().setSingle(new AdaptiveLocalSearcher(MAX_IN_MEMORY_RESULT_ENTRIES));
			tempDb.swappableLuceneSearcher().setMulti(new AdaptiveMultiSearcher(MAX_IN_MEMORY_RESULT_ENTRIES));
		}
		return index;
	}

	private void assertCount(LuceneIndex<String, String> luceneIndex, long expected) {
		Assertions.assertEquals(expected, getCount(luceneIndex));
	}

	private long getCount(LuceneIndex<String, String> luceneIndex) {
		luceneIndex.refresh(true);
		var totalHitsCount = luceneIndex.count(null, new MatchAllDocsQuery());
		Assertions.assertTrue(totalHitsCount.exact(), "Can't get count because the total hits count is not exact");
		return totalHitsCount.value();
	}

	@Test
	public void testNoOp() {
	}

	@Test
	public void testNoOpAllocation() {
		for (int i = 0; i < 10; i++) {
			var a = Buf.create(i * 512);
		}
	}

	@ParameterizedTest
	@MethodSource("provideArguments")
	public void testGetLuceneIndex(boolean shards) {
		try (var luceneIndex = getLuceneIndex(shards, null)) {
			Assertions.assertNotNull(luceneIndex);
		}
	}

	@ParameterizedTest
	@MethodSource("provideArguments")
	public void testDeleteAll(boolean shards) {
		try (var luceneIndex = getLuceneIndex(shards, null)) {
			luceneIndex.deleteAll();
			assertCount(luceneIndex, 0);
		}
	}

	@ParameterizedTest
	@MethodSource("provideArguments")
	public void testDelete(boolean shards) {
		try (var luceneIndex = getLuceneIndex(shards, null)) {
			var prevCount = getCount(luceneIndex);
			luceneIndex.deleteDocument("test-key-1");
			assertCount(luceneIndex, prevCount - 1);
		}
	}

	@ParameterizedTest
	@MethodSource("provideArguments")
	public void testUpdateSameDoc(boolean shards) {
		try (var luceneIndex = getLuceneIndex(shards, null)) {
			var prevCount = getCount(luceneIndex);
			luceneIndex.updateDocument("test-key-1", "new-value");
			assertCount(luceneIndex, prevCount);
		}
	}

	@ParameterizedTest
	@MethodSource("provideArguments")
	public void testUpdateNewDoc(boolean shards) {
		try (var luceneIndex = getLuceneIndex(shards, null)) {
			var prevCount = getCount(luceneIndex);
			luceneIndex.updateDocument("test-key-new", "new-value");
			assertCount(luceneIndex, prevCount + 1);
		}
	}

}
