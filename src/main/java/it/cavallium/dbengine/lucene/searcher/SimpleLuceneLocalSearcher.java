package it.cavallium.dbengine.lucene.searcher;

import static it.cavallium.dbengine.lucene.searcher.CurrentPageInfo.EMPTY_STATUS;
import static it.cavallium.dbengine.lucene.searcher.CurrentPageInfo.TIE_BREAKER;
import static it.cavallium.dbengine.lucene.searcher.PaginationInfo.FIRST_PAGE_LIMIT;
import static it.cavallium.dbengine.lucene.searcher.PaginationInfo.MAX_SINGLE_SEARCH_LIMIT;

import it.cavallium.dbengine.client.query.QueryParser;
import it.cavallium.dbengine.client.query.current.data.QueryParams;
import it.cavallium.dbengine.database.LLKeyScore;
import it.cavallium.dbengine.lucene.LuceneUtils;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopFieldDocs;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

public class SimpleLuceneLocalSearcher implements LuceneLocalSearcher {

	@Override
	public Mono<LuceneSearchResult> collect(IndexSearcher indexSearcher,
			LocalQueryParams queryParams,
			String keyFieldName,
			Scheduler scheduler) {
		return Mono
				.fromCallable(() -> {
					Objects.requireNonNull(queryParams.scoreMode(), "ScoreMode must not be null");
					PaginationInfo paginationInfo;
					if (queryParams.limit() <= MAX_SINGLE_SEARCH_LIMIT) {
						paginationInfo = new PaginationInfo(queryParams.limit(), queryParams.offset(), queryParams.limit(), true);
					} else {
						paginationInfo = new PaginationInfo(queryParams.limit(), queryParams.offset(), FIRST_PAGE_LIMIT, true);
					}
					//noinspection BlockingMethodInNonBlockingContext
					TopDocs firstPageTopDocs = TopDocsSearcher.getTopDocs(indexSearcher,
							queryParams.query(),
							queryParams.sort(),
							LuceneUtils.safeLongToInt(paginationInfo.firstPageOffset() + paginationInfo.firstPageLimit()),
							null,
							queryParams.scoreMode().needsScores(),
							1000,
							LuceneUtils.safeLongToInt(paginationInfo.firstPageOffset()), LuceneUtils.safeLongToInt(paginationInfo.firstPageLimit()));
					Flux<LLKeyScore> firstPageMono = LuceneMultiSearcher
							.convertHits(
									firstPageTopDocs.scoreDocs,
									IndexSearchers.unsharded(indexSearcher),
									keyFieldName,
									scheduler
							)
							.take(queryParams.limit(), true);


					Flux<LLKeyScore> nextHits = Flux.defer(() -> {
						if (paginationInfo.totalLimit() - paginationInfo.firstPageLimit() <= 0) {
							return Flux.empty();
						}
						return Flux
								.<TopDocs, CurrentPageInfo>generate(
										() -> new CurrentPageInfo(LuceneUtils.getLastScoreDoc(firstPageTopDocs.scoreDocs), paginationInfo.totalLimit() - paginationInfo.firstPageLimit(), 1),
										(s, sink) -> {
											if (s.last() != null && s.remainingLimit() > 0) {
												TopDocs pageTopDocs;
												try {
													//noinspection BlockingMethodInNonBlockingContext
													pageTopDocs = TopDocsSearcher.getTopDocs(indexSearcher, queryParams.query(),
															queryParams.sort(), s.currentPageLimit(), s.last(), queryParams.scoreMode().needsScores(), 1000);
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
								.subscribeOn(scheduler)
								.concatMap(topFieldDoc -> LuceneMultiSearcher
										.convertHits(topFieldDoc.scoreDocs, IndexSearchers.unsharded(indexSearcher), keyFieldName, scheduler)
								);
					});

					return new LuceneSearchResult(firstPageTopDocs.totalHits.value, firstPageMono.concatWith(nextHits));
				})
				.subscribeOn(scheduler);
	}
}
