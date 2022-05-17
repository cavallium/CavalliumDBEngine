package it.cavallium.dbengine;

import static it.cavallium.dbengine.DbTestUtils.MAX_IN_MEMORY_RESULT_ENTRIES;
import static it.cavallium.dbengine.DbTestUtils.destroyAllocator;
import static it.cavallium.dbengine.DbTestUtils.ensureNoLeaks;
import static it.cavallium.dbengine.DbTestUtils.newAllocator;
import static it.cavallium.dbengine.SyncUtils.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import io.netty.buffer.PooledByteBufAllocator;
import it.cavallium.dbengine.DbTestUtils.TempDb;
import it.cavallium.dbengine.DbTestUtils.TestAllocator;
import it.cavallium.dbengine.client.HitKey;
import it.cavallium.dbengine.client.Hits;
import it.cavallium.dbengine.client.LuceneIndex;
import it.cavallium.dbengine.client.Sort;
import it.cavallium.dbengine.client.LazyHitKey;
import it.cavallium.dbengine.client.query.ClientQueryParams;
import it.cavallium.dbengine.client.query.ClientQueryParamsBuilder;
import it.cavallium.dbengine.client.query.current.data.BooleanQuery;
import it.cavallium.dbengine.client.query.current.data.BooleanQueryPart;
import it.cavallium.dbengine.client.query.current.data.BoostQuery;
import it.cavallium.dbengine.client.query.current.data.MatchAllDocsQuery;
import it.cavallium.dbengine.client.query.current.data.MatchNoDocsQuery;
import it.cavallium.dbengine.client.query.current.data.OccurMust;
import it.cavallium.dbengine.client.query.current.data.OccurShould;
import it.cavallium.dbengine.client.query.current.data.Term;
import it.cavallium.dbengine.client.query.current.data.TermQuery;
import it.cavallium.dbengine.client.query.current.data.TotalHitsCount;
import it.cavallium.dbengine.database.LLLuceneIndex;
import it.cavallium.dbengine.database.disk.LLTempHugePqEnv;
import it.cavallium.dbengine.lucene.searcher.AdaptiveLocalSearcher;
import it.cavallium.dbengine.lucene.searcher.AdaptiveMultiSearcher;
import it.cavallium.dbengine.lucene.searcher.CountMultiSearcher;
import it.cavallium.dbengine.lucene.searcher.LocalSearcher;
import it.cavallium.dbengine.lucene.searcher.MultiSearcher;
import it.cavallium.dbengine.lucene.searcher.StandardSearcher;
import it.cavallium.dbengine.lucene.searcher.ScoredPagedMultiSearcher;
import it.cavallium.dbengine.lucene.searcher.PagedLocalSearcher;
import it.cavallium.dbengine.lucene.searcher.SortedScoredFullMultiSearcher;
import it.cavallium.dbengine.lucene.searcher.SortedByScoreFullMultiSearcher;
import it.cavallium.dbengine.lucene.searcher.UnsortedStreamingMultiSearcher;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.function.FailableConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink.OverflowStrategy;
import reactor.core.scheduler.Schedulers;
import reactor.util.function.Tuples;

public class TestLuceneSearches {

	private static final Logger log = LogManager.getLogger(TestLuceneSearches.class);
	private static LLTempHugePqEnv ENV;
	private static final MemoryTemporaryDbGenerator TEMP_DB_GENERATOR = new MemoryTemporaryDbGenerator();

	private static TestAllocator allocator;
	private static TempDb tempDb;
	private static LLLuceneIndex luceneSingle;
	private static LLLuceneIndex luceneMulti;
	private static LuceneIndex<String, String> multiIndex;
	private static LuceneIndex<String, String> localIndex;

	private static final Map<String, String> ELEMENTS;
	static {
		// Start the pool by creating and deleting a direct buffer
		PooledByteBufAllocator.DEFAULT.directBuffer().release();

		var modifiableElements = new LinkedHashMap<String, String>();
		modifiableElements.put("test-key-1", "0123456789");
		modifiableElements.put("test-key-2", "test 0123456789 test word");
		modifiableElements.put("test-key-3", "0123456789 test example string");
		modifiableElements.put("test-key-4", "hello world the quick brown fox jumps over the lazy dog");
		modifiableElements.put("test-key-5", "hello the quick brown fox jumps over the lazy dog");
		modifiableElements.put("test-key-6", "hello the quick brown fox jumps over the world dog");
		modifiableElements.put("test-key-7", "the quick brown fox jumps over the world dog");
		modifiableElements.put("test-key-8", "the quick brown fox jumps over the lazy dog");
		modifiableElements.put("test-key-9", "Example1");
		modifiableElements.put("test-key-10", "Example2");
		modifiableElements.put("test-key-11", "Example3");
		modifiableElements.put("test-key-12", "-234");
		modifiableElements.put("test-key-13", "2111");
		modifiableElements.put("test-key-14", "2999");
		modifiableElements.put("test-key-15", "3902");
		runVoid(Flux.range(1, 1000).doOnNext(i -> modifiableElements.put("test-key-" + (15 + i), "" + i)).then());
		ELEMENTS = Collections.unmodifiableMap(modifiableElements);
	}

	@BeforeAll
	public static void beforeAll() throws IOException {
		allocator = newAllocator();
		ensureNoLeaks(allocator.allocator(), false, false);
		tempDb = Objects.requireNonNull(TEMP_DB_GENERATOR.openTempDb(allocator).block(), "TempDB");
		luceneSingle = tempDb.luceneSingle();
		luceneMulti = tempDb.luceneMulti();
		ENV = new LLTempHugePqEnv();

		setUpIndex(true);
		setUpIndex(false);
	}

	private static void setUpIndex(boolean shards) {
		LuceneIndex<String, String> index = run(DbTestUtils.tempLuceneIndex(shards ? luceneSingle : luceneMulti));

		Flux
				.fromIterable(ELEMENTS.entrySet())
				.concatMap(entry -> index.updateDocument(entry.getKey(), entry.getValue()))
				.subscribeOn(Schedulers.boundedElastic())
				.blockLast();
		tempDb.swappableLuceneSearcher().setSingle(new CountMultiSearcher());
		tempDb.swappableLuceneSearcher().setMulti(new CountMultiSearcher());
		assertCount(index, 1000 + 15);
		if (shards) {
			multiIndex = index;
		} else {
			localIndex = index;
		}
	}

	public static Stream<Arguments> provideArguments() {
		return Stream.of(false, true).map(Arguments::of);
	}

	private static final Flux<Boolean> multi = Flux.just(false, true);
	private static final Flux<Sort> multiSort = Flux.just(
			Sort.score(),
			//todo: fix random sort field
			//Sort.randomSortField(),
			Sort.no(),
			Sort.doc(),
			Sort.numeric("longsort", false),
			Sort.numeric("longsort", true),
			Sort.numeric("intsort", false),
			Sort.numeric("intsort", true)
	);

	private static Flux<LocalSearcher> getSearchers(ExpectedQueryType info) {
		return Flux.push(sink -> {
			if (info.shard()) {
				if (info.onlyCount()) {
					sink.next(new CountMultiSearcher());
				} else {
					sink.next(new ScoredPagedMultiSearcher());
					if (info.sorted() && !info.sortedByScore()) {
						sink.next(new SortedScoredFullMultiSearcher(ENV));
					} else {
						sink.next(new SortedByScoreFullMultiSearcher(ENV));
					}
					if (!info.sorted()) {
						sink.next(new UnsortedUnscoredSimpleMultiSearcher(new PagedLocalSearcher()));
						sink.next(new UnsortedStreamingMultiSearcher());
					}
				}
				sink.next(new AdaptiveMultiSearcher(ENV, true, MAX_IN_MEMORY_RESULT_ENTRIES));
			} else {
				if (info.onlyCount()) {
					sink.next(new CountMultiSearcher());
				} else {
					sink.next(new PagedLocalSearcher());
				}
				sink.next(new AdaptiveLocalSearcher(ENV, true, MAX_IN_MEMORY_RESULT_ENTRIES));
			}
			sink.complete();
		}, OverflowStrategy.BUFFER);
	}

	public static Stream<Arguments> provideQueryArgumentsScoreMode() {
		return multi.map(tuple -> Arguments.of(multi)).toStream();
	}

	public static Stream<Arguments> provideQueryArgumentsScoreModeAndSort() {
		return multi
				.concatMap(multi -> multiSort.map(multiSort -> Tuples.of(multi, multiSort)))
				.map(tuple -> Arguments.of(tuple.toArray()))
				.toStream();
	}

	private static void runSearchers(ExpectedQueryType expectedQueryType, FailableConsumer<LocalSearcher, Throwable> consumer)
			throws Throwable {
		var searchers = run(getSearchers(expectedQueryType).collectList());
		for (LocalSearcher searcher : searchers) {
			log.info("Using searcher \"{}\"", searcher.getName());
			consumer.accept(searcher);
		}
	}

	@BeforeEach
	public void beforeEach() {
	}

	@AfterEach
	public void afterEach() {
	}

	@AfterAll
	public static void afterAll() throws IOException {
		TEMP_DB_GENERATOR.closeTempDb(tempDb).block();
		ENV.close();
		ensureNoLeaks(allocator.allocator(), true, false);
		destroyAllocator(allocator);
	}

	private LuceneIndex<String, String> getLuceneIndex(boolean shards, @Nullable LocalSearcher customSearcher) {
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
		return shards ? multiIndex : localIndex;
	}

	private static void assertCount(LuceneIndex<String, String> luceneIndex, long expected) {
		Assertions.assertEquals(expected, getCount(luceneIndex));
	}

	private static long getCount(LuceneIndex<String, String> luceneIndex) {
		luceneIndex.refresh(true).block();
		var totalHitsCount = run(luceneIndex.count(null, new MatchAllDocsQuery()));
		Assertions.assertTrue(totalHitsCount.exact(), "Can't get count because the total hits count is not exact");
		return totalHitsCount.value();
	}

	private boolean supportsPreciseHitsCount(LocalSearcher searcher,
			ClientQueryParams query) {
		var sorted = query.isSorted();
		if (searcher instanceof UnsortedStreamingMultiSearcher) {
			return false;
		} else if (!sorted) {
			return !(searcher instanceof AdaptiveMultiSearcher) && !(searcher instanceof AdaptiveLocalSearcher);
		} else {
			return true;
		}
	}

	public void testSearch(ClientQueryParamsBuilder queryParamsBuilder,
			ExpectedQueryType expectedQueryType) throws Throwable {

		runSearchers(expectedQueryType, searcher -> {
			var luceneIndex = getLuceneIndex(expectedQueryType.shard(), searcher);
			var query = queryParamsBuilder.build();
			try (var results = run(luceneIndex.search(query))) {
				var hits = results.totalHitsCount();
				var keys = getResults(results);
				if (hits.exact()) {
					Assertions.assertEquals(keys.size(), hits.value());
				} else {
					Assertions.assertTrue(keys.size() >= hits.value());
				}

				var standardSearcher = new StandardSearcher();
				luceneIndex = getLuceneIndex(expectedQueryType.shard(), standardSearcher);
				var officialQuery = queryParamsBuilder.limit(ELEMENTS.size() * 2L).build();
				try (var officialResults = run(luceneIndex.search(officialQuery))) {
					var officialHits = officialResults.totalHitsCount();
					var officialKeys = getResults(officialResults);
					if (officialHits.exact()) {
						Assertions.assertEquals(officialKeys.size(), officialHits.value());
					} else {
						Assertions.assertTrue(officialKeys.size() >= officialHits.value());
					}

					if (hits.exact() && officialHits.exact()) {
						assertExactHits(officialHits.value(), hits);
					}

					Assertions.assertEquals(officialKeys.size(), keys.size());

					assertResults(officialKeys, keys, expectedQueryType.sorted(), expectedQueryType.sortedByScore());
				}
			}
		});
	}

	@ParameterizedTest
	@MethodSource("provideQueryArgumentsScoreModeAndSort")
	public void testSearchNoDocs(boolean shards, Sort multiSort) throws Throwable {
		var queryBuilder = ClientQueryParams
				.<LazyHitKey<String>>builder()
				.query(new MatchNoDocsQuery())
				.snapshot(null)
				.computePreciseHitsCount(true)
				.sort(multiSort);

		ExpectedQueryType expectedQueryType = new ExpectedQueryType(shards, multiSort, true, false);
		testSearch(queryBuilder, expectedQueryType);
	}

	@ParameterizedTest
	@MethodSource("provideQueryArgumentsScoreModeAndSort")
	public void testSearchAllDocs(boolean shards, Sort multiSort) throws Throwable {
		var queryBuilder = ClientQueryParams
				.<LazyHitKey<String>>builder()
				.query(new MatchAllDocsQuery())
				.snapshot(null)
				.computePreciseHitsCount(true)
				.sort(multiSort);

		ExpectedQueryType expectedQueryType = new ExpectedQueryType(shards, multiSort, true, false);
		testSearch(queryBuilder, expectedQueryType);
	}

	@ParameterizedTest
	@MethodSource("provideQueryArgumentsScoreModeAndSort")
	public void testSearchAdvancedText(boolean shards, Sort multiSort) throws Throwable {
		var queryBuilder = ClientQueryParams
				.builder()
				.query(new BooleanQuery(List.of(
						new BooleanQueryPart(new BoostQuery(new TermQuery(new Term("text", "hello")), 3), new OccurShould()),
						new BooleanQueryPart(new TermQuery(new Term("text", "world")), new OccurShould()),
						new BooleanQueryPart(new BoostQuery(new TermQuery(new Term("text", "hello")), 2), new OccurShould()),
						new BooleanQueryPart(new BoostQuery(new TermQuery(new Term("text", "hello")), 100), new OccurShould()),
						new BooleanQueryPart(new TermQuery(new Term("text", "hello")), new OccurMust())
				), 1))
				.snapshot(null)
				.computePreciseHitsCount(true)
				.sort(multiSort);

		ExpectedQueryType expectedQueryType = new ExpectedQueryType(shards, multiSort, true, false);
		testSearch(queryBuilder, expectedQueryType);
	}

	private void assertResults(List<Scored> expectedKeys, List<Scored> resultKeys, boolean sorted, boolean sortedByScore) {
		if (sortedByScore) {
			Assertions.assertEquals(expectedKeys, resultKeys);
		} else if (sorted) {
			var results = resultKeys.stream().map(Scored::key).toList();
			Assertions.assertEquals(expectedKeys.stream().map(Scored::key).toList(), results);
		} else {
			var results = resultKeys.stream().map(Scored::key).collect(Collectors.toSet());
			Assertions.assertEquals(new HashSet<>(expectedKeys.stream().map(Scored::key).toList()), results);
		}
	}

	private void assertHitsIfPossible(long expectedCount, TotalHitsCount hits) {
		if (hits.exact()) {
			assertEquals(new TotalHitsCount(expectedCount, true), hits);
		}
	}

	private void assertExactHits(long expectedCount, TotalHitsCount hits) {
		assertEquals(new TotalHitsCount(expectedCount, true), hits);
	}

	private List<Scored> getResults(Hits<HitKey<String>> results) {
		return run(results
				.results()
				.map(key -> new Scored(key.key(), key.score()))
				.collectList());
	}

}
