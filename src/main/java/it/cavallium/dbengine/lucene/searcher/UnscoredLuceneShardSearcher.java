package it.cavallium.dbengine.lucene.searcher;

import static it.cavallium.dbengine.lucene.searcher.CurrentPageInfo.EMPTY_STATUS;
import static it.cavallium.dbengine.lucene.searcher.CurrentPageInfo.TIE_BREAKER;

import it.cavallium.dbengine.database.LLKeyScore;
import it.cavallium.dbengine.lucene.LuceneUtils;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopFieldDocs;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

class UnscoredLuceneShardSearcher implements LuceneShardSearcher {

	private final Object lock = new Object();
	private final List<IndexSearcher> indexSearchersArray = new ArrayList<>();
	private final List<TopDocsCollector<? extends ScoreDoc>> collectors = new ArrayList<>();
	private final CollectorManager<TopDocsCollector<? extends ScoreDoc>, ? extends TopDocs> firstPageUnsortedCollectorManager;
	private final Query luceneQuery;
	private final PaginationInfo paginationInfo;

	public UnscoredLuceneShardSearcher(CollectorManager<TopDocsCollector<? extends ScoreDoc>, ? extends TopDocs> unsortedCollectorManager,
			Query luceneQuery,
			PaginationInfo paginationInfo) {
		this.firstPageUnsortedCollectorManager = unsortedCollectorManager;
		this.luceneQuery = luceneQuery;
		this.paginationInfo = paginationInfo;
	}

	@Override
	public Mono<Void> searchOn(IndexSearcher indexSearcher, LocalQueryParams queryParams, Scheduler scheduler) {
		return Mono.<Void>fromCallable(() -> {
			TopDocsCollector<? extends ScoreDoc> collector;
			synchronized (lock) {
				//noinspection BlockingMethodInNonBlockingContext
				collector = firstPageUnsortedCollectorManager.newCollector();
				indexSearchersArray.add(indexSearcher);
				collectors.add(collector);
			}
			//noinspection BlockingMethodInNonBlockingContext
			indexSearcher.search(luceneQuery, collector);
			return null;
		}).subscribeOn(scheduler);
	}

	@Override
	public Mono<LuceneSearchResult> collect(LocalQueryParams queryParams, String keyFieldName, Scheduler scheduler) {
		return Mono
				.fromCallable(() -> {
					TopDocs[] topDocs;
					synchronized (lock) {
						if (queryParams.isSorted()) {
							topDocs = new TopFieldDocs[collectors.size()];
						} else {
							topDocs = new TopDocs[collectors.size()];
						}
						var i = 0;
						for (TopDocsCollector<?> collector : collectors) {
							topDocs[i] = collector.topDocs();
							for (ScoreDoc scoreDoc : topDocs[i].scoreDocs) {
								scoreDoc.shardIndex = i;
							}
							i++;
						}
					}
					TopDocs result = LuceneUtils.mergeTopDocs(queryParams.sort(),
							LuceneUtils.safeLongToInt(paginationInfo.firstPageOffset()),
							LuceneUtils.safeLongToInt(paginationInfo.firstPageLimit()),
							topDocs,
							TIE_BREAKER
					);
					IndexSearchers indexSearchers;
					synchronized (lock) {
						indexSearchers = IndexSearchers.of(indexSearchersArray);
					}
					Flux<LLKeyScore> firstPageHits = LuceneUtils
							.convertHits(result.scoreDocs, indexSearchers, keyFieldName, scheduler);

					Flux<LLKeyScore> nextHits = Flux.defer(() -> {
						if (paginationInfo.forceSinglePage() || paginationInfo.totalLimit() - paginationInfo.firstPageLimit() <= 0) {
							return Flux.empty();
						}
						return Flux
								.<TopDocs, CurrentPageInfo>generate(
										() -> new CurrentPageInfo(LuceneUtils.getLastScoreDoc(result.scoreDocs),
												paginationInfo.totalLimit() - paginationInfo.firstPageLimit(), 1),
										(s, sink) -> {
											if (s.last() != null && s.remainingLimit() > 0 && s.currentPageLimit() > 0) {
												Objects.requireNonNull(queryParams.scoreMode(), "ScoreMode must not be null");
												Query luceneQuery = queryParams.query();
												UnscoredCollectorManager currentPageUnsortedCollectorManager = new UnscoredCollectorManager(
														() -> TopDocsSearcher.getTopDocsCollector(queryParams.sort(), s.currentPageLimit(),
																s.last(), 1000), 0, s.currentPageLimit(), queryParams.sort());
												//noinspection BlockingMethodInNonBlockingContext
												TopDocs pageTopDocs = Flux
														.fromIterable(indexSearchersArray)
														.index()
														.flatMapSequential(tuple -> Mono
																.fromCallable(() -> {
																	long shardIndex = tuple.getT1();
																	IndexSearcher indexSearcher = tuple.getT2();
																	//noinspection BlockingMethodInNonBlockingContext
																	var results = indexSearcher.search(luceneQuery, currentPageUnsortedCollectorManager);
																	for (ScoreDoc scoreDoc : results.scoreDocs) {
																		scoreDoc.shardIndex = LuceneUtils.safeLongToInt(shardIndex);
																	}
																	return results;
																})
																.subscribeOn(scheduler)
														)
														.collect(Collectors.toCollection(ObjectArrayList::new))
														.map(topFieldDocs -> {
															if (queryParams.isSorted()) {
																@SuppressWarnings("SuspiciousToArrayCall")
																TopFieldDocs[] topFieldDocsArray = topFieldDocs.toArray(TopFieldDocs[]::new);
																return topFieldDocsArray;
															} else {
																return topFieldDocs.toArray(TopDocs[]::new);
															}
														})
														.flatMap(topFieldDocs -> Mono
																.fromCallable(() -> LuceneUtils
																		.mergeTopDocs(queryParams.sort(), 0, s.currentPageLimit(), topFieldDocs, TIE_BREAKER)
																)
																.subscribeOn(scheduler)
														)
														.blockOptional().orElseThrow();

												var pageLastDoc = LuceneUtils.getLastScoreDoc(pageTopDocs.scoreDocs);
												sink.next(pageTopDocs);
												return new CurrentPageInfo(pageLastDoc, s.remainingLimit() - s.currentPageLimit(),
														s.pageIndex() + 1);
											} else {
												sink.complete();
												return EMPTY_STATUS;
											}
										},
										s -> {}
								)
								.subscribeOn(scheduler)
								.concatMap(topFieldDoc -> LuceneUtils
										.convertHits(topFieldDoc.scoreDocs, indexSearchers, keyFieldName, scheduler)
								);
					});

					return new LuceneSearchResult(result.totalHits.value, firstPageHits
							.concatWith(nextHits)
							.transform(flux -> LuceneUtils.filterTopDoc(flux, queryParams))
					);
				})
				.subscribeOn(scheduler);
		}

}
