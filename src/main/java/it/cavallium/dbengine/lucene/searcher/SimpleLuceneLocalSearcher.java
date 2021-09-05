package it.cavallium.dbengine.lucene.searcher;

import static it.cavallium.dbengine.lucene.searcher.CurrentPageInfo.EMPTY_STATUS;
import static it.cavallium.dbengine.lucene.searcher.PaginationInfo.FIRST_PAGE_LIMIT;
import static it.cavallium.dbengine.lucene.searcher.PaginationInfo.MAX_SINGLE_SEARCH_LIMIT;

import it.cavallium.dbengine.database.LLKeyScore;
import it.cavallium.dbengine.lucene.LuceneUtils;
import java.io.IOException;
import java.util.Objects;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class SimpleLuceneLocalSearcher implements LuceneLocalSearcher {

	@Override
	public Mono<LuceneSearchResult> collect(IndexSearcher indexSearcher,
			Mono<Void> releaseIndexSearcher,
			LocalQueryParams queryParams,
			String keyFieldName) {
		return Mono
				.fromCallable(() -> {
					if (Schedulers.isInNonBlockingThread()) {
						throw new UnsupportedOperationException("Called collect in a nonblocking thread");
					}
					Objects.requireNonNull(queryParams.scoreMode(), "ScoreMode must not be null");
					PaginationInfo paginationInfo;
					if (queryParams.limit() <= MAX_SINGLE_SEARCH_LIMIT) {
						paginationInfo = new PaginationInfo(queryParams.limit(), queryParams.offset(), queryParams.limit(), true);
					} else {
						paginationInfo = new PaginationInfo(queryParams.limit(), queryParams.offset(), FIRST_PAGE_LIMIT, false);
					}
					TopDocs firstPageTopDocs;
					{
						TopDocsCollector<ScoreDoc> firstPageCollector = TopDocsSearcher.getTopDocsCollector(
								queryParams.sort(),
								LuceneUtils.safeLongToInt(paginationInfo.firstPageOffset() + paginationInfo.firstPageLimit()),
								null,
								LuceneUtils.totalHitsThreshold(),
								!paginationInfo.forceSinglePage(),
								queryParams.isScored());
						indexSearcher.search(queryParams.query(), firstPageCollector);
						firstPageTopDocs = firstPageCollector.topDocs(LuceneUtils.safeLongToInt(paginationInfo.firstPageOffset()),
								LuceneUtils.safeLongToInt(paginationInfo.firstPageLimit())
						);
					}
					Flux<LLKeyScore> firstPageMono = LuceneUtils
							.convertHits(
									firstPageTopDocs.scoreDocs,
									IndexSearchers.unsharded(indexSearcher),
									keyFieldName,
									true
							)
							.take(queryParams.limit(), true);


					Flux<LLKeyScore> nextHits;
					if (paginationInfo.forceSinglePage() || paginationInfo.totalLimit() - paginationInfo.firstPageLimit() <= 0) {
						nextHits = null;
					} else {
						nextHits = Flux.defer(() -> Flux
							.<TopDocs, CurrentPageInfo>generate(
									() -> new CurrentPageInfo(LuceneUtils.getLastScoreDoc(firstPageTopDocs.scoreDocs), paginationInfo.totalLimit() - paginationInfo.firstPageLimit(), 1),
									(s, sink) -> {
										if (Schedulers.isInNonBlockingThread()) {
											throw new UnsupportedOperationException("Called collect in a nonblocking thread");
										}
										if (s.last() != null && s.remainingLimit() > 0) {
											TopDocs pageTopDocs;
											try {
												TopDocsCollector<ScoreDoc> collector = TopDocsSearcher.getTopDocsCollector(queryParams.sort(),
														s.currentPageLimit(),
														s.last(),
														LuceneUtils.totalHitsThreshold(),
														true,
														queryParams.isScored()
												);
												indexSearcher.search(queryParams.query(), collector);
												pageTopDocs = collector.topDocs();
											} catch (IOException e) {
												sink.error(e);
												return EMPTY_STATUS;
											}
											var pageLastDoc = LuceneUtils.getLastScoreDoc(pageTopDocs.scoreDocs);
											sink.next(pageTopDocs);
											return new CurrentPageInfo(pageLastDoc, s.remainingLimit() - s.currentPageLimit(), s.pageIndex() + 1);
										} else {
											sink.complete();
											return EMPTY_STATUS;
										}
									},
									s -> {}
							)
							.flatMapSequential(topFieldDoc -> LuceneUtils
									.convertHits(topFieldDoc.scoreDocs, IndexSearchers.unsharded(indexSearcher), keyFieldName, true)
							)
						);
					}

					Flux<LLKeyScore> combinedFlux;

					if (nextHits != null) {
						combinedFlux = firstPageMono
								.concatWith(nextHits);
					} else {
						combinedFlux = firstPageMono;
					}

					return new LuceneSearchResult(LuceneUtils.convertTotalHitsCount(firstPageTopDocs.totalHits), combinedFlux,
							//.transform(flux -> LuceneUtils.filterTopDoc(flux, queryParams)),
							releaseIndexSearcher
					);
				});
	}
}
