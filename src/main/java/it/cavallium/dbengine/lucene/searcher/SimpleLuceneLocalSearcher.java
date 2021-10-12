package it.cavallium.dbengine.lucene.searcher;

import static it.cavallium.dbengine.lucene.searcher.CurrentPageInfo.EMPTY_STATUS;
import static it.cavallium.dbengine.lucene.searcher.PaginationInfo.FIRST_PAGE_LIMIT;
import static it.cavallium.dbengine.lucene.searcher.PaginationInfo.MAX_SINGLE_SEARCH_LIMIT;

import io.net5.buffer.api.Send;
import io.net5.buffer.api.internal.ResourceSupport;
import it.cavallium.dbengine.database.LLKeyScore;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.disk.LLIndexSearcher;
import it.cavallium.dbengine.database.disk.LLIndexSearchers;
import it.cavallium.dbengine.database.disk.LLIndexSearchers.UnshardedIndexSearchers;
import it.cavallium.dbengine.lucene.LuceneUtils;
import it.cavallium.dbengine.lucene.searcher.LLSearchTransformer.TransformerInput;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.TotalHits.Relation;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;
import reactor.core.scheduler.Schedulers;

public class SimpleLuceneLocalSearcher implements LuceneLocalSearcher {

	@Override
	public Mono<Send<LuceneSearchResult>> collect(Mono<Send<LLIndexSearcher>> indexSearcherMono,
			LocalQueryParams queryParams,
			String keyFieldName,
			LLSearchTransformer transformer) {

		Objects.requireNonNull(queryParams.scoreMode(), "ScoreMode must not be null");
		PaginationInfo paginationInfo = getPaginationInfo(queryParams);

		var indexSearchersMono = indexSearcherMono.map(LLIndexSearchers::unsharded).map(ResourceSupport::send);

		return LLUtils.usingSendResource(indexSearchersMono, indexSearchers -> {
					Mono<LocalQueryParams> queryParamsMono;
					if (transformer == LLSearchTransformer.NO_TRANSFORMATION) {
						queryParamsMono = Mono.just(queryParams);
					} else {
						queryParamsMono = transformer.transform(Mono
								.fromSupplier(() -> new TransformerInput(indexSearchers, queryParams)));
					}

					return queryParamsMono.flatMap(queryParams2 -> this
					// Search first page results
					.searchFirstPage(indexSearchers.shards(), queryParams2, paginationInfo)
					// Compute the results of the first page
					.transform(firstPageTopDocsMono -> this.computeFirstPageResults(firstPageTopDocsMono, indexSearchers.shards(),
							keyFieldName, queryParams2))
					// Compute other results
					.transform(firstResult -> this.computeOtherResults(firstResult, indexSearchers.shards(), queryParams2,
							keyFieldName, indexSearchers::close))
					// Ensure that one LuceneSearchResult is always returned
					.single()
			);
		},
		false);
	}

	@Override
	public String getName() {
		return "simplelocal";
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
	private Mono<PageData> searchFirstPage(List<IndexSearcher> indexSearchers,
			LocalQueryParams queryParams,
			PaginationInfo paginationInfo) {
		var limit = paginationInfo.totalLimit();
		var pagination = !paginationInfo.forceSinglePage();
		var resultsOffset = LuceneUtils.safeLongToInt(paginationInfo.firstPageOffset());
		var currentPageInfo = new CurrentPageInfo(null, limit, 0);
		return Mono
				.just(currentPageInfo)
				.<PageData>handle((s, sink) -> this.searchPageSync(queryParams, indexSearchers, pagination, resultsOffset, s, sink))
				//defaultIfEmpty(new PageData(new TopDocs(new TotalHits(0, Relation.EQUAL_TO), new ScoreDoc[0]), currentPageInfo))
				.single();
	}

	/**
	 * Compute the results of the first page, extracting useful data
	 */
	private Mono<FirstPageResults> computeFirstPageResults(Mono<PageData> firstPageDataMono,
			List<IndexSearcher> indexSearchers,
			String keyFieldName,
			LocalQueryParams queryParams) {
		return firstPageDataMono.map(firstPageData -> {
			var totalHitsCount = LuceneUtils.convertTotalHitsCount(firstPageData.topDocs().totalHits);
			var scoreDocs = firstPageData.topDocs().scoreDocs;
			assert LLUtils.isSet(scoreDocs);

			Flux<LLKeyScore> firstPageHitsFlux = LuceneUtils.convertHits(Flux.fromArray(scoreDocs),
							indexSearchers, keyFieldName, true)
					.take(queryParams.limit(), true);

			CurrentPageInfo nextPageInfo = firstPageData.nextPageInfo();

			return new FirstPageResults(totalHitsCount, firstPageHitsFlux, nextPageInfo);
		}).single();
	}

	private Mono<Send<LuceneSearchResult>> computeOtherResults(Mono<FirstPageResults> firstResultMono,
			List<IndexSearcher> indexSearchers,
			LocalQueryParams queryParams,
			String keyFieldName,
			Runnable onClose) {
		return firstResultMono.map(firstResult -> {
			var totalHitsCount = firstResult.totalHitsCount();
			var firstPageHitsFlux = firstResult.firstPageHitsFlux();
			var secondPageInfo = firstResult.nextPageInfo();

			Flux<LLKeyScore> nextHitsFlux = searchOtherPages(indexSearchers, queryParams, keyFieldName, secondPageInfo);

			Flux<LLKeyScore> combinedFlux = firstPageHitsFlux.concatWith(nextHitsFlux);
			return new LuceneSearchResult(totalHitsCount, combinedFlux, onClose).send();
		}).single();
	}

	/**
	 * Search effectively the merged raw results of the next pages
	 */
	private Flux<LLKeyScore> searchOtherPages(List<IndexSearcher> indexSearchers,
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
			List<IndexSearcher> indexSearchers,
			boolean allowPagination,
			int resultsOffset,
			CurrentPageInfo s,
			SynchronousSink<PageData> sink) {
		LLUtils.ensureBlocking();
		if (resultsOffset < 0) {
			throw new IndexOutOfBoundsException(resultsOffset);
		}
		var currentPageLimit = queryParams.pageLimits().getPageLimit(s.pageIndex());
		if (s.pageIndex() == 0 && s.remainingLimit() == 0) {
			int count;
			try {
				count = indexSearchers.get(0).count(queryParams.query());
			} catch (IOException e) {
				sink.error(e);
				return EMPTY_STATUS;
			}
			var nextPageInfo = new CurrentPageInfo(null, 0, 1);
			sink.next(new PageData(new TopDocs(new TotalHits(count, Relation.EQUAL_TO), new ScoreDoc[0]), nextPageInfo));
			return EMPTY_STATUS;
		} else if (s.pageIndex() == 0 || (s.last() != null && s.remainingLimit() > 0)) {
			TopDocs pageTopDocs;
			try {
				TopDocsCollector<ScoreDoc> collector = TopDocsSearcher.getTopDocsCollector(queryParams.sort(),
						currentPageLimit, s.last(), LuceneUtils.totalHitsThreshold(),
						allowPagination, queryParams.isScored());
				indexSearchers.get(0).search(queryParams.query(), collector);
				if (resultsOffset > 0) {
					pageTopDocs = collector.topDocs(resultsOffset, currentPageLimit);
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
				nextRemainingLimit = s.remainingLimit() - currentPageLimit;
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
