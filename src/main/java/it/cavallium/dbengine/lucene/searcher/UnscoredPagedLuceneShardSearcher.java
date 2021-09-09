package it.cavallium.dbengine.lucene.searcher;

import static it.cavallium.dbengine.lucene.searcher.CurrentPageInfo.EMPTY_STATUS;

import it.cavallium.dbengine.database.LLKeyScore;
import it.cavallium.dbengine.lucene.LuceneUtils;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.io.IOException;
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
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

class UnscoredPagedLuceneShardSearcher implements LuceneShardSearcher {

	private final Object lock = new Object();
	private final List<IndexSearcher> indexSearchersArray = new ArrayList<>();
	private final List<Mono<Void>> indexSearcherReleasersArray = new ArrayList<>();
	private final List<TopDocsCollector<ScoreDoc>> collectors = new ArrayList<>();
	private final CollectorManager<TopDocsCollector<ScoreDoc>, TopDocs> firstPageUnsortedCollectorManager;
	private final Query luceneQuery;
	private final PaginationInfo paginationInfo;

	public UnscoredPagedLuceneShardSearcher(
			CollectorManager<TopDocsCollector<ScoreDoc>, TopDocs> firstPagensortedCollectorManager,
			Query luceneQuery,
			PaginationInfo paginationInfo) {
		this.firstPageUnsortedCollectorManager = firstPagensortedCollectorManager;
		this.luceneQuery = luceneQuery;
		this.paginationInfo = paginationInfo;
	}

	@Override
	public Mono<Void> searchOn(IndexSearcher indexSearcher,
			Mono<Void> releaseIndexSearcher,
			LocalQueryParams queryParams,
			Scheduler scheduler) {
		return Mono.<Void>fromCallable(() -> {
			if (Schedulers.isInNonBlockingThread()) {
				throw new UnsupportedOperationException("Called searchOn in a nonblocking thread");
			}
			TopDocsCollector<ScoreDoc> collector;
			synchronized (lock) {
				//noinspection BlockingMethodInNonBlockingContext
				collector = firstPageUnsortedCollectorManager.newCollector();
				indexSearchersArray.add(indexSearcher);
				indexSearcherReleasersArray.add(releaseIndexSearcher);
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
					if (Schedulers.isInNonBlockingThread()) {
						throw new UnsupportedOperationException("Called collect in a nonblocking thread");
					}
					TopDocs result;
					Mono<Void> release;
					synchronized (lock) {
						//noinspection BlockingMethodInNonBlockingContext
						result = firstPageUnsortedCollectorManager.reduce(collectors);
						release = Mono.when(indexSearcherReleasersArray);
					}
					IndexSearchers indexSearchers;
					synchronized (lock) {
						indexSearchers = IndexSearchers.of(indexSearchersArray);
					}
					Flux<LLKeyScore> firstPageHits = LuceneUtils
							.convertHits(Flux.fromArray(result.scoreDocs), indexSearchers, keyFieldName, scheduler, false);

					Flux<LLKeyScore> nextHits = Flux
							.<TopDocs, CurrentPageInfo>generate(
									() -> new CurrentPageInfo(LuceneUtils.getLastScoreDoc(result.scoreDocs),
											paginationInfo.totalLimit() - paginationInfo.firstPageLimit(), 1),
									(s, sink) -> {
										if (s.last() != null && s.remainingLimit() > 0 && s.currentPageLimit() > 0) {
											Objects.requireNonNull(queryParams.scoreMode(), "ScoreMode must not be null");
											Query luceneQuery = queryParams.query();
											int perShardCollectorLimit = s.currentPageLimit() / indexSearchersArray.size();
											UnscoredTopDocsCollectorManager currentPageUnsortedCollectorManager
													= new UnscoredTopDocsCollectorManager(
															() -> TopDocsSearcher.getTopDocsCollector(queryParams.sort(), perShardCollectorLimit,
																	s.last(), LuceneUtils.totalHitsThreshold(), true, queryParams.isScored()),
													0, s.currentPageLimit(), queryParams.sort());

											try {
												var collectors = new ObjectArrayList<TopDocsCollector<ScoreDoc>>(indexSearchersArray.size());
												for (IndexSearcher indexSearcher : indexSearchersArray) {
													//noinspection BlockingMethodInNonBlockingContext
													var collector = currentPageUnsortedCollectorManager.newCollector();
													//noinspection BlockingMethodInNonBlockingContext
													indexSearcher.search(luceneQuery, collector);

													collectors.add(collector);
												}
												//noinspection BlockingMethodInNonBlockingContext
												TopDocs pageTopDocs = currentPageUnsortedCollectorManager.reduce(collectors);
												var pageLastDoc = LuceneUtils.getLastScoreDoc(pageTopDocs.scoreDocs);

												sink.next(pageTopDocs);
												return new CurrentPageInfo(pageLastDoc, s.remainingLimit() - s.currentPageLimit(),
														s.pageIndex() + 1);
											} catch (IOException ex) {
												sink.error(ex);
												return EMPTY_STATUS;
											}
										} else {
											sink.complete();
											return EMPTY_STATUS;
										}
									}
							)
							.subscribeOn(scheduler)
							.flatMapSequential(topFieldDoc -> LuceneUtils.convertHits(Flux.fromArray(topFieldDoc.scoreDocs),
									indexSearchers, keyFieldName, scheduler, false), 2)
							.transform(flux -> {
								if (paginationInfo.forceSinglePage()
										|| paginationInfo.totalLimit() - paginationInfo.firstPageLimit() <= 0) {
									return Flux.empty();
								} else {
									return flux;
								}
							});

					return new LuceneSearchResult(LuceneUtils.convertTotalHitsCount(result.totalHits), firstPageHits
							.concatWith(nextHits),
							//.transform(flux -> LuceneUtils.filterTopDoc(flux, queryParams)),
							release
					);
				})
				.subscribeOn(scheduler);
		}

}
