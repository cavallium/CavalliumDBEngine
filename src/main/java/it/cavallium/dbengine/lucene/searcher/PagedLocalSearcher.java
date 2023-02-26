package it.cavallium.dbengine.lucene.searcher;

import static it.cavallium.dbengine.lucene.searcher.CurrentPageInfo.EMPTY_STATUS;
import static it.cavallium.dbengine.lucene.searcher.PaginationInfo.MAX_SINGLE_SEARCH_LIMIT;
import static it.cavallium.dbengine.utils.StreamUtils.fastListing;
import static it.cavallium.dbengine.utils.StreamUtils.streamWhileNonNull;

import it.cavallium.dbengine.database.LLKeyScore;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.disk.LLIndexSearcher;
import it.cavallium.dbengine.database.disk.LLIndexSearchers;
import it.cavallium.dbengine.lucene.LuceneUtils;
import it.cavallium.dbengine.lucene.collector.TopDocsCollectorMultiManager;
import it.cavallium.dbengine.utils.DBException;
import it.cavallium.dbengine.utils.StreamUtils;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.TotalHits.Relation;
import org.jetbrains.annotations.Nullable;

public class PagedLocalSearcher implements LocalSearcher {

	private static final Logger LOG = LogManager.getLogger(PagedLocalSearcher.class);

	@Override
	public LuceneSearchResult collect(LLIndexSearcher indexSearcher,
			LocalQueryParams queryParams,
			@Nullable String keyFieldName,
			GlobalQueryRewrite transformer,
			Function<Stream<LLKeyScore>, Stream<LLKeyScore>> filterer) {
		if (transformer != GlobalQueryRewrite.NO_REWRITE) {
			return LuceneUtils.rewrite(this, indexSearcher, queryParams, keyFieldName, transformer, filterer);
		}
		PaginationInfo paginationInfo = getPaginationInfo(queryParams);

		var indexSearchers = LLIndexSearchers.unsharded(indexSearcher);

		// Search first page results
		var firstPageTopDocs = this.searchFirstPage(indexSearchers.shards(), queryParams, paginationInfo);
		// Compute the results of the first page
		var firstResult = this.computeFirstPageResults(firstPageTopDocs,
				indexSearchers.shards(),
				keyFieldName,
				queryParams
		);
		return this.computeOtherResults(firstResult, indexSearchers.shards(), queryParams, keyFieldName, filterer);
	}

	@Override
	public String getName() {
		return "paged local";
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
	private PageData searchFirstPage(List<IndexSearcher> indexSearchers,
			LocalQueryParams queryParams,
			PaginationInfo paginationInfo) {
		var limit = paginationInfo.totalLimit();
		var pagination = !paginationInfo.forceSinglePage();
		var resultsOffset = LuceneUtils.safeLongToInt(paginationInfo.firstPageOffset());
		var currentPageInfo = new CurrentPageInfo(null, limit, 0);
		return this.searchPageSync(queryParams, indexSearchers, pagination, resultsOffset, currentPageInfo).pageData();
	}

	/**
	 * Compute the results of the first page, extracting useful data
	 */
	private FirstPageResults computeFirstPageResults(PageData firstPageData,
			List<IndexSearcher> indexSearchers,
			String keyFieldName,
			LocalQueryParams queryParams) {
		var totalHitsCount = LuceneUtils.convertTotalHitsCount(firstPageData.topDocs().totalHits);
		var scoreDocs = firstPageData.topDocs().scoreDocs;
		assert LLUtils.isSet(scoreDocs);

		Stream<LLKeyScore> firstPageHitsFlux = LuceneUtils.convertHits(Stream.of(scoreDocs),
						indexSearchers, keyFieldName
				)
				.limit(queryParams.limitLong());

		CurrentPageInfo nextPageInfo = firstPageData.nextPageInfo();

		return new FirstPageResults(totalHitsCount, firstPageHitsFlux, nextPageInfo);
	}

	private LuceneSearchResult computeOtherResults(FirstPageResults firstResult,
			List<IndexSearcher> indexSearchers,
			LocalQueryParams queryParams,
			String keyFieldName,
			Function<Stream<LLKeyScore>, Stream<LLKeyScore>> filterer) {
		var totalHitsCount = firstResult.totalHitsCount();
		var firstPageHitsStream = firstResult.firstPageHitsStream();
		var secondPageInfo = firstResult.nextPageInfo();

		Stream<LLKeyScore> nextHitsFlux = searchOtherPages(indexSearchers, queryParams, keyFieldName, secondPageInfo);

		Stream<LLKeyScore> combinedFlux = Stream.concat(firstPageHitsStream, nextHitsFlux);
		return new LuceneSearchResult(totalHitsCount, StreamUtils.collect(filterer.apply(combinedFlux), fastListing()));
	}

	/**
	 * Search effectively the merged raw results of the next pages
	 */
	private Stream<LLKeyScore> searchOtherPages(List<IndexSearcher> indexSearchers,
			LocalQueryParams queryParams, String keyFieldName, CurrentPageInfo secondPageInfo) {
		AtomicReference<CurrentPageInfo> pageInfo = new AtomicReference<>(secondPageInfo);
		Object lock = new Object();
		Stream<ScoreDoc> topFieldDocFlux = streamWhileNonNull(() -> {
			synchronized (lock) {
				var currentPageInfo = pageInfo.getPlain();
				var result = searchPageSync(queryParams, indexSearchers, true, 0, currentPageInfo);
				pageInfo.setPlain(result.nextPageToIterate());
				return result.pageData();
			}
		}).flatMap(pd -> Stream.of(pd.topDocs().scoreDocs));
		return LuceneUtils.convertHits(topFieldDocFlux, indexSearchers, keyFieldName);
	}

	/**
	 *
	 * @param resultsOffset offset of the resulting topDocs. Useful if you want to
	 *                       skip the first n results in the first page
	 */
	private PageIterationStepResult searchPageSync(LocalQueryParams queryParams,
			List<IndexSearcher> indexSearchers,
			boolean allowPagination,
			int resultsOffset,
			CurrentPageInfo s) {
		if (resultsOffset < 0) {
			throw new IndexOutOfBoundsException(resultsOffset);
		}
		PageData result = null;
		var currentPageLimit = queryParams.pageLimits().getPageLimit(s.pageIndex());
		if (s.pageIndex() == 0 && s.remainingLimit() == 0) {
			int count;
			try {
				count = indexSearchers.get(0).count(queryParams.query());
			} catch (IOException e) {
				throw new DBException(e);
			}
			var nextPageInfo = new CurrentPageInfo(null, 0, 1);
			return new PageIterationStepResult(EMPTY_STATUS, new PageData(new TopDocs(new TotalHits(count, Relation.EQUAL_TO), new ScoreDoc[0]), nextPageInfo));
		} else if (s.pageIndex() == 0 || (s.last() != null && s.remainingLimit() > 0)) {
			TopDocs pageTopDocs;
			try {
				var cmm = new TopDocsCollectorMultiManager(queryParams.sort(),
						currentPageLimit, s.last(), queryParams.getTotalHitsThresholdInt(),
						allowPagination, queryParams.needsScores(), resultsOffset, currentPageLimit);

				pageTopDocs = cmm.reduce(List.of(indexSearchers
						.get(0)
						.search(queryParams.query(), cmm.get(queryParams.query(), indexSearchers.get(0)))));
			} catch (IOException e) {
				throw new DBException(e);
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
			return new PageIterationStepResult(nextPageInfo, new PageData(pageTopDocs, nextPageInfo));
		} else {
			return new PageIterationStepResult(EMPTY_STATUS, null);
		}
	}
}
