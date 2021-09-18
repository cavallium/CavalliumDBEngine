package it.cavallium.dbengine.lucene.searcher;

import static it.cavallium.dbengine.lucene.searcher.CurrentPageInfo.EMPTY_STATUS;
import static it.cavallium.dbengine.lucene.searcher.PaginationInfo.FIRST_PAGE_LIMIT;
import static it.cavallium.dbengine.lucene.searcher.PaginationInfo.MAX_SINGLE_SEARCH_LIMIT;

import io.net5.buffer.api.Buffer;
import io.net5.buffer.api.Resource;
import io.net5.buffer.api.Send;
import it.cavallium.dbengine.client.query.current.data.TotalHitsCount;
import it.cavallium.dbengine.database.LLKeyScore;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.disk.LLIndexSearcher;
import it.cavallium.dbengine.lucene.LuceneUtils;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TotalHits;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuple3;
import reactor.util.function.Tuples;

public class SimpleLuceneLocalSearcher implements LuceneLocalSearcher {

	@Override
	public Mono<Send<LuceneSearchResult>> collect(Mono<Send<LLIndexSearcher>> indexSearcherMono,
			LocalQueryParams queryParams,
			String keyFieldName) {

		Objects.requireNonNull(queryParams.scoreMode(), "ScoreMode must not be null");
		PaginationInfo paginationInfo;
		if (queryParams.limit() <= MAX_SINGLE_SEARCH_LIMIT) {
			paginationInfo = new PaginationInfo(queryParams.limit(), queryParams.offset(), queryParams.limit(), true);
		} else {
			paginationInfo = new PaginationInfo(queryParams.limit(), queryParams.offset(), FIRST_PAGE_LIMIT, false);
		}

		return indexSearcherMono
				.flatMap(indexSearcherToReceive -> {
					var indexSearcher = indexSearcherToReceive.receive();
					var indexSearchers = IndexSearchers.unsharded(indexSearcher);
					return Mono
							.fromCallable(() -> {
								LLUtils.ensureBlocking();
								TopDocsCollector<ScoreDoc> firstPageCollector = TopDocsSearcher.getTopDocsCollector(
										queryParams.sort(),
										LuceneUtils.safeLongToInt(paginationInfo.firstPageOffset() + paginationInfo.firstPageLimit()),
										null,
										LuceneUtils.totalHitsThreshold(),
										!paginationInfo.forceSinglePage(),
										queryParams.isScored());

								indexSearcher.getIndexSearcher().search(queryParams.query(), firstPageCollector);
								return firstPageCollector.topDocs(LuceneUtils.safeLongToInt(paginationInfo.firstPageOffset()),
										LuceneUtils.safeLongToInt(paginationInfo.firstPageLimit())
								);
							})
							.map(firstPageTopDocs -> {
								Flux<LLKeyScore> firstPageHitsFlux = LuceneUtils.convertHits(Flux.fromArray(firstPageTopDocs.scoreDocs),
												indexSearchers, keyFieldName, true)
										.take(queryParams.limit(), true);

								return Tuples.of(Optional.ofNullable(LuceneUtils.getLastScoreDoc(firstPageTopDocs.scoreDocs)),
										LuceneUtils.convertTotalHitsCount(firstPageTopDocs.totalHits), firstPageHitsFlux);
							})
							.map(firstResult -> {
								var firstPageLastScoreDoc = firstResult.getT1();
								var totalHitsCount = firstResult.getT2();
								var firstPageFlux = firstResult.getT3();


								Flux<LLKeyScore> nextHits;
								if (paginationInfo.forceSinglePage() || paginationInfo.totalLimit() - paginationInfo.firstPageLimit() <= 0) {
									nextHits = null;
								} else {
									nextHits = Flux.defer(() -> Flux
											.<TopDocs, CurrentPageInfo>generate(
													() -> new CurrentPageInfo(firstPageLastScoreDoc.orElse(null), paginationInfo.totalLimit() - paginationInfo.firstPageLimit(), 1),
													(s, sink) -> {
														LLUtils.ensureBlocking();
														if (s.last() != null && s.remainingLimit() > 0) {
															TopDocs pageTopDocs;
															try {
																TopDocsCollector<ScoreDoc> collector = TopDocsSearcher.getTopDocsCollector(queryParams.sort(),
																		s.currentPageLimit(), s.last(), LuceneUtils.totalHitsThreshold(), true,
																		queryParams.isScored());
																indexSearcher.getIndexSearcher().search(queryParams.query(), collector);
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
											.subscribeOn(Schedulers.boundedElastic())
											.flatMapIterable(topDocs -> Arrays.asList(topDocs.scoreDocs))
											.transform(topFieldDocFlux -> LuceneUtils.convertHits(topFieldDocFlux, indexSearchers,
													keyFieldName, true))
									);
								}

								Flux<LLKeyScore> combinedFlux;

								if (nextHits != null) {
									combinedFlux = firstPageFlux
											.concatWith(nextHits);
								} else {
									combinedFlux = firstPageFlux;
								}
								return new LuceneSearchResult(totalHitsCount, combinedFlux, d -> {
									indexSearcher.close();
									indexSearchers.close();
								}).send();
							})
							.doFinally(s -> {
								// Close searchers if the search result has not been returned
								if (s != SignalType.ON_COMPLETE) {
									indexSearcher.close();
									indexSearchers.close();
								}
							});
						}
				)
				.doOnDiscard(Send.class, Send::close);
	}
}
