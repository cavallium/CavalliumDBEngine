package it.cavallium.dbengine;

import static it.cavallium.dbengine.DbTestUtils.MAX_IN_MEMORY_RESULT_ENTRIES;
import static it.cavallium.dbengine.DbTestUtils.destroyAllocator;
import static it.cavallium.dbengine.DbTestUtils.ensureNoLeaks;
import static it.cavallium.dbengine.DbTestUtils.newAllocator;
import static it.cavallium.dbengine.SyncUtils.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import it.cavallium.dbengine.DbTestUtils.TempDb;
import it.cavallium.dbengine.DbTestUtils.TestAllocator;
import it.cavallium.dbengine.client.LuceneIndex;
import it.cavallium.dbengine.client.Sort;
import it.cavallium.dbengine.client.query.current.data.MatchAllDocsQuery;
import it.cavallium.dbengine.database.LLLuceneIndex;
import it.cavallium.dbengine.database.LLScoreMode;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.disk.LLTempHugePqEnv;
import it.cavallium.dbengine.lucene.searcher.AdaptiveLocalSearcher;
import it.cavallium.dbengine.lucene.searcher.AdaptiveMultiSearcher;
import it.cavallium.dbengine.lucene.searcher.CountMultiSearcher;
import it.cavallium.dbengine.lucene.searcher.LocalSearcher;
import it.cavallium.dbengine.lucene.searcher.MultiSearcher;
import java.io.IOException;
import java.util.Objects;
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
import reactor.core.publisher.Flux;
import reactor.util.function.Tuples;

public class TestLuceneIndex {

	private final Logger log = LogManager.getLogger(this.getClass());
	private static LLTempHugePqEnv ENV;

	private TestAllocator allocator;
	private TempDb tempDb;
	private LLLuceneIndex luceneSingle;
	private LLLuceneIndex luceneMulti;

	protected TemporaryDbGenerator getTempDbGenerator() {
		return new MemoryTemporaryDbGenerator();
	}

	@BeforeAll
	public static void beforeAll() throws IOException {
		ENV = new LLTempHugePqEnv();
	}

	@BeforeEach
	public void beforeEach() {
		this.allocator = newAllocator();
		ensureNoLeaks(allocator.allocator(), false, false);
		tempDb = Objects.requireNonNull(getTempDbGenerator().openTempDb(allocator).block(), "TempDB");
		luceneSingle = tempDb.luceneSingle();
		luceneMulti = tempDb.luceneMulti();
	}

	public static Stream<Arguments> provideArguments() {
		return Stream.of(false, true).map(Arguments::of);
	}

	private static final Flux<Boolean> multi = Flux.just(false, true);
	private static final Flux<LLScoreMode> scoreModes = Flux.just(LLScoreMode.NO_SCORES,
			LLScoreMode.TOP_SCORES,
			LLScoreMode.COMPLETE_NO_SCORES,
			LLScoreMode.COMPLETE
	);
	private static final Flux<Sort> multiSort = Flux.just(Sort.score(),
			Sort.random(),
			Sort.no(),
			Sort.doc(),
			Sort.numeric("longsort", false),
			Sort.numeric("longsort", true)
	);

	public static Stream<Arguments> provideQueryArgumentsScoreMode() {
		return multi
				.concatMap(shard -> scoreModes.map(scoreMode -> Tuples.of(shard, scoreMode)))
				.map(tuple -> Arguments.of(tuple.toArray()))
				.toStream();
	}

	public static Stream<Arguments> provideQueryArgumentsSort() {
		return multi
				.concatMap(shard -> multiSort.map(multiSort -> Tuples.of(shard, multiSort)))
				.map(tuple -> Arguments.of(tuple.toArray()))
				.toStream();
	}

	public static Stream<Arguments> provideQueryArgumentsScoreModeAndSort() {
		return multi
				.concatMap(shard -> scoreModes.map(scoreMode -> Tuples.of(shard, scoreMode)))
				.concatMap(tuple -> multiSort.map(multiSort -> Tuples.of(tuple.getT1(), tuple.getT2(), multiSort)))
				.map(tuple -> Arguments.of(tuple.toArray()))
				.toStream();
	}

	@AfterEach
	public void afterEach() {
		getTempDbGenerator().closeTempDb(tempDb).block();
		ensureNoLeaks(allocator.allocator(), true, false);
		destroyAllocator(allocator);
	}

	@AfterAll
	public static void afterAll() throws IOException {
		ENV.close();
	}

	private LuceneIndex<String, String> getLuceneIndex(boolean shards, @Nullable LocalSearcher customSearcher) {
		LuceneIndex<String, String> index = run(DbTestUtils.tempLuceneIndex(shards ? luceneSingle : luceneMulti));
		index.updateDocument("test-key-1", "0123456789").block();
		index.updateDocument("test-key-2", "test 0123456789 test word").block();
		index.updateDocument("test-key-3", "0123456789 test example string").block();
		index.updateDocument("test-key-4", "hello world the quick brown fox jumps over the lazy dog").block();
		index.updateDocument("test-key-5", "hello the quick brown fox jumps over the lazy dog").block();
		index.updateDocument("test-key-6", "hello the quick brown fox jumps over the world dog").block();
		index.updateDocument("test-key-7", "the quick brown fox jumps over the world dog").block();
		index.updateDocument("test-key-8", "the quick brown fox jumps over the lazy dog").block();
		index.updateDocument("test-key-9", "Example1").block();
		index.updateDocument("test-key-10", "Example2").block();
		index.updateDocument("test-key-11", "Example3").block();
		index.updateDocument("test-key-12", "-234").block();
		index.updateDocument("test-key-13", "2111").block();
		index.updateDocument("test-key-14", "2999").block();
		index.updateDocument("test-key-15", "3902").block();
		Flux
				.range(1, 1000)
				.concatMap(i -> index.updateDocument("test-key-" + (15 + i), "" + i))
				.transform(LLUtils::handleDiscard)
				.blockLast();
		tempDb.swappableLuceneSearcher().setSingle(new CountMultiSearcher());
		tempDb.swappableLuceneSearcher().setMulti(new CountMultiSearcher());
		assertCount(index, 1000 + 15);
		if (customSearcher != null) {
			tempDb.swappableLuceneSearcher().setSingle(customSearcher);
			if (shards) {
				if (customSearcher instanceof MultiSearcher multiSearcher) {
					tempDb.swappableLuceneSearcher().setMulti(multiSearcher);
				} else {
					throw new IllegalArgumentException("Expected a LuceneMultiSearcher, got a LuceneLocalSearcher: " + customSearcher.getName());
				}
			}
		} else {
			tempDb.swappableLuceneSearcher().setSingle(new AdaptiveLocalSearcher(ENV, true, MAX_IN_MEMORY_RESULT_ENTRIES));
			tempDb.swappableLuceneSearcher().setMulti(new AdaptiveMultiSearcher(ENV, true, MAX_IN_MEMORY_RESULT_ENTRIES));
		}
		return index;
	}

	private void assertCount(LuceneIndex<String, String> luceneIndex, long expected) {
		Assertions.assertEquals(expected, getCount(luceneIndex));
	}

	private long getCount(LuceneIndex<String, String> luceneIndex) {
		luceneIndex.refresh(true).block();
		var totalHitsCount = run(luceneIndex.count(null, new MatchAllDocsQuery()));
		Assertions.assertTrue(totalHitsCount.exact(), "Can't get count because the total hits count is not exact");
		return totalHitsCount.value();
	}

	@Test
	public void testNoOp() {
	}

	@Test
	public void testNoOpAllocation() {
		for (int i = 0; i < 10; i++) {
			var a = allocator.allocator().allocate(i * 512);
			a.send().receive().close();
		}
	}

	@ParameterizedTest
	@MethodSource("provideArguments")
	public void testGetLuceneIndex(boolean shards) {
		var luceneIndex = getLuceneIndex(shards, null);
		Assertions.assertNotNull(luceneIndex);
	}

	@ParameterizedTest
	@MethodSource("provideArguments")
	public void testDeleteAll(boolean shards) {
		var luceneIndex = getLuceneIndex(shards, null);
		runVoid(luceneIndex.deleteAll());
		assertCount(luceneIndex, 0);
	}

	@ParameterizedTest
	@MethodSource("provideArguments")
	public void testDelete(boolean shards) {
		var luceneIndex = getLuceneIndex(shards, null);
		var prevCount = getCount(luceneIndex);
		runVoid(luceneIndex.deleteDocument("test-key-1"));
		assertCount(luceneIndex, prevCount - 1);
	}

	@ParameterizedTest
	@MethodSource("provideArguments")
	public void testUpdateSameDoc(boolean shards) {
		var luceneIndex = getLuceneIndex(shards, null);
		var prevCount = getCount(luceneIndex);
		runVoid(luceneIndex.updateDocument("test-key-1", "new-value"));
		assertCount(luceneIndex, prevCount );
	}

	@ParameterizedTest
	@MethodSource("provideArguments")
	public void testUpdateNewDoc(boolean shards) {
		var luceneIndex = getLuceneIndex(shards, null);
		var prevCount = getCount(luceneIndex);
		runVoid(luceneIndex.updateDocument("test-key-new", "new-value"));
		assertCount(luceneIndex, prevCount + 1);
	}

}
