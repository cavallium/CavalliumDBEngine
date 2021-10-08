package it.cavallium.dbengine.lucene.searcher;

import static it.cavallium.dbengine.lucene.searcher.PaginationInfo.FIRST_PAGE_LIMIT;
import static it.cavallium.dbengine.lucene.searcher.PaginationInfo.MAX_SINGLE_SEARCH_LIMIT;

import io.net5.buffer.api.Send;
import it.cavallium.dbengine.database.LLKeyScore;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.disk.LLIndexSearcher;
import it.cavallium.dbengine.database.disk.LLIndexSearchers;
import it.cavallium.dbengine.database.disk.LLLocalGroupedReactiveRocksIterator;
import it.cavallium.dbengine.lucene.LuceneUtils;
import it.cavallium.dbengine.lucene.searcher.LLSearchTransformer.TransformerInput;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Sort;
import org.warp.commonutils.log.Logger;
import org.warp.commonutils.log.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class ScoredSimpleLuceneShardSearcher implements LuceneMultiSearcher {

	protected static final Logger logger = LoggerFactory.getLogger(ScoredSimpleLuceneShardSearcher.class);

	public ScoredSimpleLuceneShardSearcher() {
	}

	@Override
	public Mono<Send<LuceneSearchResult>> collectMulti(Mono<Send<LLIndexSearchers>> indexSearchersMono,
			LocalQueryParams queryParams,
			String keyFieldName,
			LLSearchTransformer transformer) {
		Mono<LocalQueryParams> queryParamsMono;
		if (transformer == LLSearchTransformer.NO_TRANSFORMATION) {
			queryParamsMono = Mono.just(queryParams);
		} else {
			queryParamsMono = LLUtils.usingSendResource(indexSearchersMono, indexSearchers -> transformer.transform(Mono
					.fromSupplier(() -> new TransformerInput(indexSearchers, queryParams))), true);
		}

		return queryParamsMono.flatMap(queryParams2 -> {
			Objects.requireNonNull(queryParams2.scoreMode(), "ScoreMode must not be null");
			PaginationInfo paginationInfo = getPaginationInfo(queryParams2);

			return LLUtils.usingSendResource(indexSearchersMono, indexSearchers -> this
							// Search first page results
							.searchFirstPage(indexSearchers.shards(), queryParams2, paginationInfo)
							// Compute the results of the first page
							.transform(firstPageTopDocsMono -> this.computeFirstPageResults(firstPageTopDocsMono, indexSearchers,
									keyFieldName, queryParams2))
							// Compute other results
							.map(firstResult -> this.computeOtherResults(firstResult, indexSearchers.shards(),
									queryParams2, keyFieldName, indexSearchers::close))
							// Ensure that one LuceneSearchResult is always returned
							.single(),
					false);
		});
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
			return new PaginationInfo(queryParams.limit(), queryParams.offset(), queryParams.pageLimits(), true);
		} else {
			return new PaginationInfo(queryParams.limit(), queryParams.offset(), queryParams.pageLimits(), false);
		}
	}

	/**
	 * Search effectively the raw results of the first page
	 */
	private Mono<PageData> searchFirstPage(Iterable<IndexSearcher> indexSearchers,
			LocalQueryParams queryParams,
			PaginationInfo paginationInfo) {
		var limit = paginationInfo.totalLimit();
		var pageLimits = paginationInfo.pageLimits();
		var pagination = !paginationInfo.forceSinglePage();
		var resultsOffset = LuceneUtils.safeLongToInt(paginationInfo.firstPageOffset());
		return Mono
				.fromSupplier(() -> new CurrentPageInfo(null, limit, 0))
				.flatMap(s -> this.searchPage(queryParams, indexSearchers, pagination, pageLimits, resultsOffset, s));
	}

	/**
	 * Compute the results of the first page, extracting useful data
	 */
	private Mono<FirstPageResults> computeFirstPageResults(Mono<PageData> firstPageDataMono,
			LLIndexSearchers indexSearchers,
			String keyFieldName,
			LocalQueryParams queryParams) {
		return firstPageDataMono.map(firstPageData -> {
			var totalHitsCount = LuceneUtils.convertTotalHitsCount(firstPageData.topDocs().totalHits);
			var scoreDocs = firstPageData.topDocs().scoreDocs;
			assert LLUtils.isSet(scoreDocs);

			Flux<LLKeyScore> firstPageHitsFlux = LuceneUtils.convertHits(Flux.fromArray(scoreDocs),
							indexSearchers.shards(), keyFieldName, true)
					.take(queryParams.limit(), true);

			CurrentPageInfo nextPageInfo = firstPageData.nextPageInfo();

			return new FirstPageResults(totalHitsCount, firstPageHitsFlux, nextPageInfo);
		});
	}

	private Send<LuceneSearchResult> computeOtherResults(FirstPageResults firstResult,
			List<IndexSearcher> indexSearchers,
			LocalQueryParams queryParams,
			String keyFieldName,
			Runnable onClose) {
		var totalHitsCount = firstResult.totalHitsCount();
		var firstPageHitsFlux = firstResult.firstPageHitsFlux();
		var secondPageInfo = firstResult.nextPageInfo();

		Flux<LLKeyScore> nextHitsFlux = searchOtherPages(indexSearchers, queryParams, keyFieldName, secondPageInfo);

		Flux<LLKeyScore> combinedFlux = firstPageHitsFlux.concatWith(nextHitsFlux);
		return new LuceneSearchResult(totalHitsCount, combinedFlux, onClose).send();
	}

	/**
	 * Search effectively the merged raw results of the next pages
	 */
	private Flux<LLKeyScore> searchOtherPages(List<IndexSearcher> indexSearchers,
			LocalQueryParams queryParams, String keyFieldName, CurrentPageInfo secondPageInfo) {
		return Flux
				.defer(() -> {
					AtomicReference<CurrentPageInfo> currentPageInfoRef = new AtomicReference<>(secondPageInfo);
					return Mono
							.fromSupplier(currentPageInfoRef::get)
							.doOnNext(s -> logger.debug("Current page info: {}", s))
							.flatMap(currentPageInfo -> this.searchPage(queryParams, indexSearchers, true,
									queryParams.pageLimits(), 0, currentPageInfo))
							.doOnNext(s -> logger.debug("Next page info: {}", s.nextPageInfo()))
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
			Iterable<IndexSearcher> indexSearchers,
			boolean allowPagination,
			PageLimits pageLimits,
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
						var pageLimit = pageLimits.getPageLimit(s.pageIndex());
						var after = (FieldDoc) s.last();
						var totalHitsThreshold = LuceneUtils.totalHitsThreshold();
						return new ScoringShardsCollectorManager(sort, pageLimit, after, totalHitsThreshold,
								resultsOffset);
					} else {
						return null;
					}
				})
				.flatMap(sharedManager -> Flux
						.fromIterable(indexSearchers)
						.flatMap(shard -> Mono.fromCallable(() -> {
							LLUtils.ensureBlocking();
							var collector = sharedManager.newCollector();
							shard.search(queryParams.query(), collector);
							return collector;
						}))
						.collectList()
						.flatMap(collectors -> Mono.fromCallable(() -> {
							LLUtils.ensureBlocking();
							var pageTopDocs = sharedManager.reduce(collectors);
							var pageLastDoc = LuceneUtils.getLastScoreDoc(pageTopDocs.scoreDocs);
							long nextRemainingLimit;
							if (allowPagination) {
								nextRemainingLimit = s.remainingLimit() - pageLimits.getPageLimit(s.pageIndex());
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
