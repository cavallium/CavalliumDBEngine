package it.cavallium.dbengine.lucene.searcher;

import static it.cavallium.dbengine.client.UninterruptibleScheduler.uninterruptibleScheduler;
import static it.cavallium.dbengine.database.LLUtils.singleOrClose;
import static it.cavallium.dbengine.lucene.searcher.GlobalQueryRewrite.NO_REWRITE;
import static it.cavallium.dbengine.lucene.searcher.PaginationInfo.MAX_SINGLE_SEARCH_LIMIT;

import io.netty5.buffer.api.Send;
import it.cavallium.dbengine.database.LLKeyScore;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.disk.LLIndexSearchers;
import it.cavallium.dbengine.lucene.LuceneUtils;
import it.cavallium.dbengine.lucene.PageLimits;
import it.cavallium.dbengine.lucene.collector.ScoringShardsCollectorMultiManager;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Sort;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class ScoredPagedMultiSearcher implements MultiSearcher {

	protected static final Logger LOG = LogManager.getLogger(ScoredPagedMultiSearcher.class);

	public ScoredPagedMultiSearcher() {
	}

	@Override
	public Mono<LuceneSearchResult> collectMulti(Mono<LLIndexSearchers> indexSearchersMono,
			LocalQueryParams queryParams,
			@Nullable String keyFieldName,
			GlobalQueryRewrite transformer) {
		if (transformer != GlobalQueryRewrite.NO_REWRITE) {
			return LuceneUtils.rewriteMulti(this, indexSearchersMono, queryParams, keyFieldName, transformer);
		}

		PaginationInfo paginationInfo = getPaginationInfo(queryParams);

		return singleOrClose(indexSearchersMono, indexSearchers -> this
				// Search first page results
				.searchFirstPage(indexSearchers.shards(), queryParams, paginationInfo)
				// Compute the results of the first page
				.transform(firstPageTopDocsMono ->
						this.computeFirstPageResults(firstPageTopDocsMono, indexSearchers, keyFieldName, queryParams
				))
				// Compute other results
				.map(firstResult -> this.computeOtherResults(firstResult, indexSearchers.shards(), queryParams, keyFieldName,
						() -> {
							try {
								indexSearchers.close();
							} catch (UncheckedIOException e) {
								LOG.error("Can't close index searchers", e);
							}
						}
				))
				// Ensure that one LuceneSearchResult is always returned
				.single());
	}

	private Sort getSort(LocalQueryParams queryParams) {
		return queryParams.sort();
	}

	/**
	 * Get the pagination info
	 */
	private PaginationInfo getPaginationInfo(LocalQueryParams queryParams) {
		if (queryParams.limitInt() <= MAX_SINGLE_SEARCH_LIMIT) {
			return new PaginationInfo(queryParams.limitInt(), queryParams.offsetInt(), queryParams.pageLimits(), true);
		} else {
			return new PaginationInfo(queryParams.limitInt(), queryParams.offsetInt(), queryParams.pageLimits(), false);
		}
	}

	/**
	 * Search effectively the raw results of the first page
	 */
	private Mono<PageData> searchFirstPage(List<IndexSearcher> indexSearchers,
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
					.take(queryParams.limitInt(), true);

			CurrentPageInfo nextPageInfo = firstPageData.nextPageInfo();

			return new FirstPageResults(totalHitsCount, firstPageHitsFlux, nextPageInfo);
		});
	}

	private LuceneSearchResult computeOtherResults(FirstPageResults firstResult,
			List<IndexSearcher> indexSearchers,
			LocalQueryParams queryParams,
			String keyFieldName,
			Runnable onClose) {
		var totalHitsCount = firstResult.totalHitsCount();
		var firstPageHitsFlux = firstResult.firstPageHitsFlux();
		var secondPageInfo = firstResult.nextPageInfo();

		Flux<LLKeyScore> nextHitsFlux = searchOtherPages(indexSearchers, queryParams, keyFieldName, secondPageInfo);

		Flux<LLKeyScore> combinedFlux = firstPageHitsFlux.concatWith(nextHitsFlux);
		return new LuceneSearchResult(totalHitsCount, combinedFlux, onClose);
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
							.doOnNext(s -> LOG.trace("Current page info: {}", s))
							.flatMap(currentPageInfo -> this.searchPage(queryParams, indexSearchers, true,
									queryParams.pageLimits(), 0, currentPageInfo))
							.doOnNext(s -> LOG.trace("Next page info: {}", s.nextPageInfo()))
							.doOnNext(s -> currentPageInfoRef.set(s.nextPageInfo()))
							.repeatWhen(s -> s.takeWhile(n -> n > 0));
				})
				.subscribeOn(uninterruptibleScheduler(Schedulers.boundedElastic()))
				.publishOn(Schedulers.parallel())
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
			List<IndexSearcher> indexSearchers,
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
					if (s.pageIndex() == 0 || (s.last() != null && s.remainingLimit() > 0)) {
						var query = queryParams.query();
						@Nullable var sort = getSort(queryParams);
						var pageLimit = pageLimits.getPageLimit(s.pageIndex());
						var after = (FieldDoc) s.last();
						var totalHitsThreshold = queryParams.getTotalHitsThresholdInt();
						return new ScoringShardsCollectorMultiManager(query, sort, pageLimit, after, totalHitsThreshold,
								resultsOffset, pageLimit);
					} else {
						return null;
					}
				})
				.subscribeOn(Schedulers.boundedElastic())
				.flatMap(cmm -> Flux
						.fromIterable(indexSearchers)
						.index()
						.flatMap(shardWithIndex -> Mono.fromCallable(() -> {
							LLUtils.ensureBlocking();

							var index = (int) (long) shardWithIndex.getT1();
							var shard = shardWithIndex.getT2();

							var cm = cmm.get(shard, index);

							return shard.search(queryParams.query(), cm);
						}).subscribeOn(uninterruptibleScheduler(Schedulers.boundedElastic())))
						.publishOn(Schedulers.parallel())
						.collectList()
						.flatMap(results -> Mono.fromCallable(() -> {
							LLUtils.ensureBlocking();
							var pageTopDocs = cmm.reduce(results);

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
						}).subscribeOn(uninterruptibleScheduler(Schedulers.boundedElastic())))
				)
				.publishOn(Schedulers.parallel());
	}

	@Override
	public String getName() {
		return "scored paged multi";
	}
}
