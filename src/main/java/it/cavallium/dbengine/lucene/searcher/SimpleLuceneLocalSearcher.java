package it.cavallium.dbengine.lucene.searcher;

import static it.cavallium.dbengine.lucene.searcher.CurrentPageInfo.EMPTY_STATUS;
import static it.cavallium.dbengine.lucene.searcher.PaginationInfo.FIRST_PAGE_LIMIT;
import static it.cavallium.dbengine.lucene.searcher.PaginationInfo.MAX_SINGLE_SEARCH_LIMIT;

import io.net5.buffer.api.Send;
import it.cavallium.dbengine.database.LLKeyScore;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.disk.LLIndexContext;
import it.cavallium.dbengine.database.disk.LLIndexContexts;
import it.cavallium.dbengine.lucene.LuceneUtils;
import it.cavallium.dbengine.database.disk.LLIndexContexts.UnshardedIndexSearchers;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;
import reactor.core.scheduler.Schedulers;

public class SimpleLuceneLocalSearcher implements LuceneLocalSearcher {

	@Override
	public Mono<Send<LuceneSearchResult>> collect(Mono<Send<LLIndexContext>> indexSearcherMono,
			LocalQueryParams queryParams,
			String keyFieldName) {

		Objects.requireNonNull(queryParams.scoreMode(), "ScoreMode must not be null");
		PaginationInfo paginationInfo = getPaginationInfo(queryParams);

		var indexSearchersMono = indexSearcherMono.map(LLIndexContexts::unsharded);

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
	private Mono<PageData> searchFirstPage(UnshardedIndexSearchers indexSearchers,
			LocalQueryParams queryParams,
			PaginationInfo paginationInfo) {
		var limit = LuceneUtils.safeLongToInt(paginationInfo.firstPageOffset() + paginationInfo.firstPageLimit());
		var pagination = !paginationInfo.forceSinglePage();
		var resultsOffset = LuceneUtils.safeLongToInt(paginationInfo.firstPageOffset());
		return Mono
				.fromSupplier(() -> new CurrentPageInfo(null, limit, 0))
				.handle((s, sink) -> this.searchPageSync(queryParams, indexSearchers, pagination, resultsOffset, s, sink));
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
			UnshardedIndexSearchers indexSearchers,
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
	private Flux<LLKeyScore> searchOtherPages(UnshardedIndexSearchers indexSearchers,
			LocalQueryParams queryParams, String keyFieldName, CurrentPageInfo secondPageInfo) {
		return Flux
				.<PageData, CurrentPageInfo>generate(
						() -> secondPageInfo,
						(s, sink) -> searchPageSync(queryParams, indexSearchers, true, 0, s, sink),
						s -> {}
				)
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
	private CurrentPageInfo searchPageSync(LocalQueryParams queryParams,
			UnshardedIndexSearchers indexSearchers,
			boolean allowPagination,
			int resultsOffset,
			CurrentPageInfo s,
			SynchronousSink<PageData> sink) {
		LLUtils.ensureBlocking();
		if (resultsOffset < 0) {
			throw new IndexOutOfBoundsException(resultsOffset);
		}
		if ((s.pageIndex() == 0 || s.last() != null) && s.remainingLimit() > 0) {
			TopDocs pageTopDocs;
			try {
				TopDocsCollector<ScoreDoc> collector = TopDocsSearcher.getTopDocsCollector(queryParams.sort(),
						s.currentPageLimit(), s.last(), LuceneUtils.totalHitsThreshold(), allowPagination,
						queryParams.isScored());
				indexSearchers.shard().getIndexSearcher().search(queryParams.query(), collector);
				if (resultsOffset > 0) {
					pageTopDocs = collector.topDocs(resultsOffset, s.currentPageLimit());
				} else {
					pageTopDocs = collector.topDocs();
				}
			} catch (IOException e) {
				sink.error(e);
				return EMPTY_STATUS;
			}
			var pageLastDoc = LuceneUtils.getLastScoreDoc(pageTopDocs.scoreDocs);
			long nextRemainingLimit;
			if (allowPagination) {
				nextRemainingLimit = s.remainingLimit() - s.currentPageLimit();
			} else {
				nextRemainingLimit = 0L;
			}
			var nextPageIndex = s.pageIndex() + 1;
			var nextPageInfo = new CurrentPageInfo(pageLastDoc, nextRemainingLimit, nextPageIndex);
			sink.next(new PageData(pageTopDocs, nextPageInfo));
			return nextPageInfo;
		} else {
			sink.complete();
			return EMPTY_STATUS;
		}
	}
}
