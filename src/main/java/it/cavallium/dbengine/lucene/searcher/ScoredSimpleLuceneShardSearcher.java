package it.cavallium.dbengine.lucene.searcher;

import static it.cavallium.dbengine.lucene.searcher.PaginationInfo.FIRST_PAGE_LIMIT;
import static it.cavallium.dbengine.lucene.searcher.PaginationInfo.MAX_SINGLE_SEARCH_LIMIT;

import io.net5.buffer.api.Send;
import it.cavallium.dbengine.database.LLKeyScore;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.disk.LLIndexContext;
import it.cavallium.dbengine.database.disk.LLIndexContexts;
import it.cavallium.dbengine.lucene.LuceneUtils;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.lucene.search.Sort;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class ScoredSimpleLuceneShardSearcher implements LuceneMultiSearcher {

	public ScoredSimpleLuceneShardSearcher() {
	}

	@Override
	public Mono<Send<LuceneSearchResult>> collect(Flux<Send<LLIndexContext>> indexSearchersFlux,
			LocalQueryParams queryParams,
			String keyFieldName) {
		Objects.requireNonNull(queryParams.scoreMode(), "ScoreMode must not be null");
		PaginationInfo paginationInfo = getPaginationInfo(queryParams);

		var indexSearchersMono = indexSearchersFlux.collectList().map(LLIndexContexts::of);

		return LLUtils.usingResource(indexSearchersMono, indexSearchers -> this
						// Search first page results
						.searchFirstPage(indexSearchers, queryParams, paginationInfo)
						// Compute the results of the first page
						.transform(firstPageTopDocsMono -> this.computeFirstPageResults(firstPageTopDocsMono, indexSearchers,
								keyFieldName, queryParams))
						// Compute other results
						.transform(firstResult -> this.computeOtherResults(firstResult, indexSearchers, queryParams, keyFieldName))
						// Ensure that one LuceneSearchResult is always returned
						.single(),
				false);
	}

	private Sort getSort(LocalQueryParams queryParams) {
		Sort luceneSort = queryParams.sort();
		if (luceneSort == null) {
			luceneSort = Sort.RELEVANCE;
		}
		return luceneSort;
	}

	/**
	 * Get the pagination info
	 */
	private PaginationInfo getPaginationInfo(LocalQueryParams queryParams) {
		if (queryParams.limit() <= MAX_SINGLE_SEARCH_LIMIT) {
			return new PaginationInfo(queryParams.limit(), queryParams.offset(), queryParams.limit(), true);
		} else {
			return new PaginationInfo(queryParams.limit(), queryParams.offset(), FIRST_PAGE_LIMIT, false);
		}
	}

	/**
	 * Search effectively the raw results of the first page
	 */
	private Mono<PageData> searchFirstPage(LLIndexContexts indexSearchers,
			LocalQueryParams queryParams,
			PaginationInfo paginationInfo) {
		var limit = LuceneUtils.safeLongToInt(paginationInfo.firstPageOffset() + paginationInfo.firstPageLimit());
		var pagination = !paginationInfo.forceSinglePage();
		var resultsOffset = LuceneUtils.safeLongToInt(paginationInfo.firstPageOffset());
		return Mono
				.fromSupplier(() -> new CurrentPageInfo(null, limit, 0))
				.flatMap(s -> this.searchPage(queryParams, indexSearchers, pagination, resultsOffset, s));
	}

	/**
	 * Compute the results of the first page, extracting useful data
	 */
	private Mono<FirstPageResults> computeFirstPageResults(Mono<PageData> firstPageDataMono,
			LLIndexContexts indexSearchers,
			String keyFieldName,
			LocalQueryParams queryParams) {
		return firstPageDataMono.map(firstPageData -> {
			var totalHitsCount = LuceneUtils.convertTotalHitsCount(firstPageData.topDocs().totalHits);

			Flux<LLKeyScore> firstPageHitsFlux = LuceneUtils.convertHits(Flux.fromArray(firstPageData.topDocs().scoreDocs),
							indexSearchers, keyFieldName, true)
					.take(queryParams.limit(), true);

			CurrentPageInfo nextPageInfo = firstPageData.nextPageInfo();

			return new FirstPageResults(totalHitsCount, firstPageHitsFlux, nextPageInfo);
		});
	}

	private Mono<Send<LuceneSearchResult>> computeOtherResults(Mono<FirstPageResults> firstResultMono,
			LLIndexContexts indexSearchers,
			LocalQueryParams queryParams,
			String keyFieldName) {
		return firstResultMono.map(firstResult -> {
			var totalHitsCount = firstResult.totalHitsCount();
			var firstPageHitsFlux = firstResult.firstPageHitsFlux();
			var secondPageInfo = firstResult.nextPageInfo();

			Flux<LLKeyScore> nextHitsFlux = searchOtherPages(indexSearchers, queryParams, keyFieldName, secondPageInfo);

			Flux<LLKeyScore> combinedFlux = firstPageHitsFlux.concatWith(nextHitsFlux);
			return new LuceneSearchResult(totalHitsCount, combinedFlux, d -> indexSearchers.close()).send();
		});
	}

	/**
	 * Search effectively the merged raw results of the next pages
	 */
	private Flux<LLKeyScore> searchOtherPages(LLIndexContexts indexSearchers,
			LocalQueryParams queryParams, String keyFieldName, CurrentPageInfo secondPageInfo) {
		return Flux
				.defer(() -> {
					AtomicReference<CurrentPageInfo> currentPageInfoRef = new AtomicReference<>(secondPageInfo);
					return Flux
							.defer(() -> searchPage(queryParams, indexSearchers, true, 0, currentPageInfoRef.get()))
							.doOnNext(s -> currentPageInfoRef.set(s.nextPageInfo()))
							.repeatWhen(s -> s.takeWhile(n -> n > 0));
				})
				.subscribeOn(Schedulers.boundedElastic())
				.map(PageData::topDocs)
				.flatMapIterable(topDocs -> Arrays.asList(topDocs.scoreDocs))
				.transform(topFieldDocFlux -> LuceneUtils.convertHits(topFieldDocFlux, indexSearchers,
						keyFieldName, true));
	}

	/**
	 *
	 * @param resultsOffset offset of the resulting topDocs. Useful if you want to
	 *                       skip the first n results in the first page
	 */
	private Mono<PageData> searchPage(LocalQueryParams queryParams,
			LLIndexContexts indexSearchers,
			boolean allowPagination,
			int resultsOffset,
			CurrentPageInfo s) {
		return Mono
				.fromCallable(() -> {
					LLUtils.ensureBlocking();
					if (resultsOffset < 0) {
						throw new IndexOutOfBoundsException(resultsOffset);
					}
					if ((s.pageIndex() == 0 || s.last() != null) && s.remainingLimit() > 0) {
						var sort = getSort(queryParams);
						var limit = s.currentPageLimit();
						var totalHitsThreshold = LuceneUtils.totalHitsThreshold();
						return new ScoringShardsCollectorManager(sort, limit, null,
								totalHitsThreshold, resultsOffset, s.currentPageLimit());
					} else {
						return null;
					}
				})
				.flatMap(sharedManager -> Flux
						.fromIterable(indexSearchers.shards())
						.flatMap(shard -> Mono.fromCallable(() -> {
							var collector = sharedManager.newCollector();
							shard.getIndexSearcher().search(queryParams.query(), collector);
							return collector;
						}))
						.collectList()
						.flatMap(collectors -> Mono.fromCallable(() -> {
							var pageTopDocs = sharedManager.reduce(collectors);
							var pageLastDoc = LuceneUtils.getLastScoreDoc(pageTopDocs.scoreDocs);
							long nextRemainingLimit;
							if (allowPagination) {
								nextRemainingLimit = s.remainingLimit() - s.currentPageLimit();
							} else {
								nextRemainingLimit = 0L;
							}
							var nextPageIndex = s.pageIndex() + 1;
							var nextPageInfo = new CurrentPageInfo(pageLastDoc, nextRemainingLimit, nextPageIndex);
							return new PageData(pageTopDocs, nextPageInfo);
						}))
				);
	}
}
