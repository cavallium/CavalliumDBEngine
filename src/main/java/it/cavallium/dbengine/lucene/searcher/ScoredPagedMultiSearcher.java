package it.cavallium.dbengine.lucene.searcher;

import static it.cavallium.dbengine.lucene.searcher.PaginationInfo.MAX_SINGLE_SEARCH_LIMIT;
import static it.cavallium.dbengine.utils.StreamUtils.LUCENE_SCHEDULER;
import static it.cavallium.dbengine.utils.StreamUtils.fastListing;
import static it.cavallium.dbengine.utils.StreamUtils.streamWhileNonNull;
import static it.cavallium.dbengine.utils.StreamUtils.toListOn;

import com.google.common.collect.Streams;
import it.cavallium.dbengine.client.query.current.data.TotalHitsCount;
import it.cavallium.dbengine.database.LLKeyScore;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.disk.LLIndexSearchers;
import it.cavallium.dbengine.lucene.LuceneCloseable;
import it.cavallium.dbengine.lucene.LuceneUtils;
import it.cavallium.dbengine.lucene.PageLimits;
import it.cavallium.dbengine.lucene.collector.ScoringShardsCollectorMultiManager;
import it.cavallium.dbengine.utils.DBException;
import it.cavallium.dbengine.utils.StreamUtils;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.jetbrains.annotations.Nullable;

public class ScoredPagedMultiSearcher implements MultiSearcher {

	protected static final Logger LOG = LogManager.getLogger(ScoredPagedMultiSearcher.class);

	public ScoredPagedMultiSearcher() {
	}

	@Override
	public LuceneSearchResult collectMulti(LLIndexSearchers indexSearchers,
			LocalQueryParams queryParams,
			@Nullable String keyFieldName,
			GlobalQueryRewrite transformer,
			Function<Stream<LLKeyScore>, Stream<LLKeyScore>> filterer) {
		if (transformer != GlobalQueryRewrite.NO_REWRITE) {
			return LuceneUtils.rewriteMulti(this, indexSearchers, queryParams, keyFieldName, transformer, filterer);
		}

		PaginationInfo paginationInfo = getPaginationInfo(queryParams);
		// Search first page results
		var firstPageTopDocs = this.searchFirstPage(indexSearchers.shards(), queryParams, paginationInfo);
				// Compute the results of the first page
		var firstResult = this.computeFirstPageResults(firstPageTopDocs, indexSearchers, keyFieldName, queryParams);
				// Compute other results
				return this.computeOtherResults(firstResult,
						indexSearchers.shards(),
						queryParams,
						keyFieldName,
						filterer
				);
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
	private PageData searchFirstPage(List<IndexSearcher> indexSearchers,
			LocalQueryParams queryParams,
			PaginationInfo paginationInfo) {
		var limit = paginationInfo.totalLimit();
		var pageLimits = paginationInfo.pageLimits();
		var pagination = !paginationInfo.forceSinglePage();
		var resultsOffset = LuceneUtils.safeLongToInt(paginationInfo.firstPageOffset());
		return this.searchPage(queryParams,
				indexSearchers,
				pagination,
				pageLimits,
				resultsOffset,
				new CurrentPageInfo(null, limit, 0)
		);
	}

	/**
	 * Compute the results of the first page, extracting useful data
	 */
	private FirstPageResults computeFirstPageResults(PageData firstPageData,
			LLIndexSearchers indexSearchers,
			String keyFieldName,
			LocalQueryParams queryParams) {
		var totalHitsCount = LuceneUtils.convertTotalHitsCount(firstPageData.topDocs().totalHits);
		var scoreDocs = firstPageData.topDocs().scoreDocs;
		assert LLUtils.isSet(scoreDocs);

		Stream<LLKeyScore> firstPageHitsFlux = LuceneUtils
				.convertHits(Stream.of(scoreDocs), indexSearchers.shards(), keyFieldName)
				.limit(queryParams.limitInt());

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

		Stream<LLKeyScore> combinedStream = Stream.concat(firstPageHitsStream, nextHitsFlux);
		return new LuceneSearchResult(totalHitsCount, StreamUtils.collect(filterer.apply(combinedStream), fastListing()));
	}

	/**
	 * Search effectively the merged raw results of the next pages
	 */
	private Stream<LLKeyScore> searchOtherPages(List<IndexSearcher> indexSearchers,
			LocalQueryParams queryParams, String keyFieldName, CurrentPageInfo secondPageInfo) {
		AtomicReference<CurrentPageInfo> currentPageInfoRef = new AtomicReference<>(secondPageInfo);
		Stream<ScoreDoc> topFieldDocStream = streamWhileNonNull(() -> {
			var currentPageInfo = currentPageInfoRef.getPlain();
			if (currentPageInfo == null) return null;
			LOG.trace("Current page info: {}", currentPageInfo);
			var result = this.searchPage(queryParams, indexSearchers, true, queryParams.pageLimits(), 0, currentPageInfo);
			LOG.trace("Next page info: {}", result != null ? result.nextPageInfo() : null);
			currentPageInfoRef.setPlain(result != null ? result.nextPageInfo() : null);
			if (result == null || result.topDocs().scoreDocs.length == 0) {
				return null;
			} else {
				return Arrays.asList(result.topDocs().scoreDocs);
			}
		}).flatMap(Collection::stream);

		return LuceneUtils.convertHits(topFieldDocStream, indexSearchers, keyFieldName);
	}

	/**
	 *
	 * @param resultsOffset offset of the resulting topDocs. Useful if you want to
	 *                       skip the first n results in the first page
	 */
	private PageData searchPage(LocalQueryParams queryParams,
			List<IndexSearcher> indexSearchers,
			boolean allowPagination,
			PageLimits pageLimits,
			int resultsOffset,
			CurrentPageInfo s) {
		if (resultsOffset < 0) {
			throw new IndexOutOfBoundsException(resultsOffset);
		}
		ScoringShardsCollectorMultiManager cmm;
		if (s.pageIndex() == 0 || (s.last() != null && s.remainingLimit() > 0)) {
			var query = queryParams.query();
			@Nullable var sort = getSort(queryParams);
			var pageLimit = pageLimits.getPageLimit(s.pageIndex());
			var after = (FieldDoc) s.last();
			var totalHitsThreshold = queryParams.getTotalHitsThresholdInt();
			cmm = new ScoringShardsCollectorMultiManager(query, sort, pageLimit, after, totalHitsThreshold,
					resultsOffset, pageLimit);
		} else {
			return null;
		};
		record IndexedShard(IndexSearcher indexSearcher, long shardIndex) {}
		List<TopDocs> shardResults = toListOn(LUCENE_SCHEDULER,
				Streams.mapWithIndex(indexSearchers.stream(), IndexedShard::new).map(shardWithIndex -> {
					var index = (int) shardWithIndex.shardIndex();
					var shard = shardWithIndex.indexSearcher();

					var cm = cmm.get(shard, index);

					try {
						return shard.search(queryParams.query(), cm);
					} catch (IOException e) {
						throw new DBException(e);
					}
				})
		);

		var pageTopDocs = cmm.reduce(shardResults);

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
	}

	@Override
	public String getName() {
		return "scored paged multi";
	}

}
