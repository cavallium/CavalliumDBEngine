package it.cavallium.dbengine;

import static it.cavallium.dbengine.DbTestUtils.destroyAllocator;
import static it.cavallium.dbengine.DbTestUtils.ensureNoLeaks;
import static it.cavallium.dbengine.DbTestUtils.newAllocator;
import static it.cavallium.dbengine.SyncUtils.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import io.net5.buffer.PooledByteBufAllocator;
import it.cavallium.dbengine.DbTestUtils.TempDb;
import it.cavallium.dbengine.DbTestUtils.TestAllocator;
import it.cavallium.dbengine.client.LuceneIndex;
import it.cavallium.dbengine.client.MultiSort;
import it.cavallium.dbengine.client.SearchResultKey;
import it.cavallium.dbengine.client.SearchResultKeys;
import it.cavallium.dbengine.client.query.BasicType;
import it.cavallium.dbengine.client.query.ClientQueryParams;
import it.cavallium.dbengine.client.query.ClientQueryParamsBuilder;
import it.cavallium.dbengine.client.query.QueryParser;
import it.cavallium.dbengine.client.query.current.data.MatchAllDocsQuery;
import it.cavallium.dbengine.client.query.current.data.MatchNoDocsQuery;
import it.cavallium.dbengine.client.query.current.data.NoSort;
import it.cavallium.dbengine.client.query.current.data.ScoreSort;
import it.cavallium.dbengine.client.query.current.data.TotalHitsCount;
import it.cavallium.dbengine.database.LLLuceneIndex;
import it.cavallium.dbengine.database.LLScoreMode;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.lucene.searcher.AdaptiveLocalSearcher;
import it.cavallium.dbengine.lucene.searcher.AdaptiveMultiSearcher;
import it.cavallium.dbengine.lucene.searcher.CountLocalSearcher;
import it.cavallium.dbengine.lucene.searcher.LocalSearcher;
import it.cavallium.dbengine.lucene.searcher.MultiSearcher;
import it.cavallium.dbengine.lucene.searcher.OfficialSearcher;
import it.cavallium.dbengine.lucene.searcher.ScoredPagedMultiSearcher;
import it.cavallium.dbengine.lucene.searcher.PagedLocalSearcher;
import it.cavallium.dbengine.lucene.searcher.SortedScoredFullMultiSearcher;
import it.cavallium.dbengine.lucene.searcher.UnsortedUnscoredSimpleMultiSearcher;
import it.cavallium.dbengine.lucene.searcher.UnsortedScoredFullMultiSearcher;
import it.cavallium.dbengine.lucene.searcher.UnsortedUnscoredStreamingMultiSearcher;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.function.FailableConsumer;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.warp.commonutils.log.Logger;
import org.warp.commonutils.log.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink.OverflowStrategy;
import reactor.core.scheduler.Schedulers;
import reactor.util.function.Tuples;

public class TestLuceneSearches {

	private static final Logger log = LoggerFactory.getLogger(TestLuceneSearches.class);
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

		var modifiableElements = new HashMap<String, String>();
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
	public static void beforeAll() {
		allocator = newAllocator();
		ensureNoLeaks(allocator.allocator(), false, false);
		tempDb = Objects.requireNonNull(TEMP_DB_GENERATOR.openTempDb(allocator).block(), "TempDB");
		luceneSingle = tempDb.luceneSingle();
		luceneMulti = tempDb.luceneMulti();

		setUpIndex(true);
		setUpIndex(false);
	}

	private static void setUpIndex(boolean shards) {
		LuceneIndex<String, String> index = run(DbTestUtils.tempLuceneIndex(shards ? luceneSingle : luceneMulti));

		Flux
				.fromIterable(ELEMENTS.entrySet())
				.flatMap(entry -> index.updateDocument(entry.getKey(), entry.getValue()))
				.subscribeOn(Schedulers.boundedElastic())
				.blockLast();
		tempDb.swappableLuceneSearcher().setSingle(new CountLocalSearcher());
		tempDb.swappableLuceneSearcher().setMulti(new UnsortedUnscoredSimpleMultiSearcher(new CountLocalSearcher()));
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
	private static final Flux<MultiSort<SearchResultKey<String>>> multiSort = Flux.just(MultiSort.topScore(),
			//todo: fix random sort field
			//MultiSort.randomSortField(),
			MultiSort.noSort(),
			MultiSort.docSort(),
			MultiSort.numericSort("longsort", false),
			MultiSort.numericSort("longsort", true)
	);

	private static Flux<LocalSearcher> getSearchers(ExpectedQueryType info) {
		return Flux.push(sink -> {
			try {
				if (info.shard()) {
					if (info.onlyCount()) {
						sink.next(new UnsortedUnscoredSimpleMultiSearcher(new CountLocalSearcher()));
					} else {
						sink.next(new ScoredPagedMultiSearcher());
						if (info.sorted() && !info.sortedByScore()) {
							sink.next(new SortedScoredFullMultiSearcher());
						} else {
							sink.next(new UnsortedScoredFullMultiSearcher());
						}
						if (!info.sorted()) {
							sink.next(new UnsortedUnscoredSimpleMultiSearcher(new PagedLocalSearcher()));
							sink.next(new UnsortedUnscoredStreamingMultiSearcher());
						}
					}
					sink.next(new AdaptiveMultiSearcher());
				} else {
					if (info.onlyCount()) {
						sink.next(new CountLocalSearcher());
					} else {
						sink.next(new PagedLocalSearcher());
					}
					sink.next(new AdaptiveLocalSearcher());
				}
				sink.complete();
			} catch (IOException e) {
				sink.error(e);
			}
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

	@AfterAll
	public static void afterAll() {
		TEMP_DB_GENERATOR.closeTempDb(tempDb).block();
		ensureNoLeaks(allocator.allocator(), true, false);
		destroyAllocator(allocator);
	}

	private LuceneIndex<String, String> getLuceneIndex(boolean shards, @Nullable LocalSearcher customSearcher) {
		try {
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
				tempDb.swappableLuceneSearcher().setSingle(new AdaptiveLocalSearcher());
				tempDb.swappableLuceneSearcher().setMulti(new AdaptiveMultiSearcher());
			}
		} catch (IOException e) {
			fail(e);
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
			ClientQueryParams<SearchResultKey<String>> query) {
		var sorted = query.isSorted();
		if (searcher instanceof UnsortedUnscoredStreamingMultiSearcher) {
			return false;
		} else if (!sorted) {
			return !(searcher instanceof AdaptiveMultiSearcher) && !(searcher instanceof AdaptiveLocalSearcher);
		} else {
			return true;
		}
	}

	public void testSearch(ClientQueryParamsBuilder<SearchResultKey<String>> queryParamsBuilder,
			ExpectedQueryType expectedQueryType) throws Throwable {

		runSearchers(expectedQueryType, searcher -> {
			var luceneIndex = getLuceneIndex(expectedQueryType.shard(), searcher);
			var query = queryParamsBuilder.build();
			try (var results = run(luceneIndex.search(query)).receive()) {
				var hits = results.totalHitsCount();
				var keys = getResults(results);
				if (hits.exact()) {
					Assertions.assertEquals(keys.size(), hits.value());
				} else {
					Assertions.assertTrue(keys.size() >= hits.value());
				}

				var officialSearcher = new OfficialSearcher();
				luceneIndex = getLuceneIndex(expectedQueryType.shard(), officialSearcher);
				var officialQuery = queryParamsBuilder.limit(ELEMENTS.size() * 2L).build();
				try (var officialResults = run(luceneIndex.search(officialQuery)).receive()) {
					var officialHits = officialResults.totalHitsCount();
					var officialKeys = getResults(officialResults).stream().toList();
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
	public void testSearchNoDocs(boolean shards, MultiSort<SearchResultKey<String>> multiSort) throws Throwable {
		ClientQueryParamsBuilder<SearchResultKey<String>> queryBuilder = ClientQueryParams.builder();
		queryBuilder.query(new MatchNoDocsQuery());
		queryBuilder.snapshot(null);
		queryBuilder.complete(true);
		queryBuilder.sort(multiSort);

		ExpectedQueryType expectedQueryType = new ExpectedQueryType(shards, multiSort, true, false);
		testSearch(queryBuilder, expectedQueryType);
	}

	@ParameterizedTest
	@MethodSource("provideQueryArgumentsScoreModeAndSort")
	public void testSearchAllDocs(boolean shards, MultiSort<SearchResultKey<String>> multiSort) throws Throwable {
		ClientQueryParamsBuilder<SearchResultKey<String>> queryBuilder = ClientQueryParams.builder();
		queryBuilder.query(new MatchAllDocsQuery());
		queryBuilder.snapshot(null);
		queryBuilder.complete(true);
		queryBuilder.sort(multiSort);

		ExpectedQueryType expectedQueryType = new ExpectedQueryType(shards, multiSort, true, false);
		testSearch(queryBuilder, expectedQueryType);
	}

	private void assertResults(List<Scored> expectedKeys, List<Scored> resultKeys, boolean sorted, boolean sortedByScore) {

		if (sortedByScore) {
			float lastScore = Float.NEGATIVE_INFINITY;
			for (Scored resultKey : resultKeys) {
				if (!Float.isNaN(resultKey.score())) {
					Assertions.assertTrue(resultKey.score() >= lastScore);
					lastScore = resultKey.score();
				}
			}
		}

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

	private List<Scored> getResults(SearchResultKeys<String> results) {
		return run(results
				.results()
				.flatMapSequential(searchResultKey -> searchResultKey
						.key()
						.single()
						.map(key -> new Scored(key, searchResultKey.score()))
				)
				.collectList());
	}

}
