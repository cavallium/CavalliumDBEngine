package it.cavallium.dbengine.lucene.searcher;

import static it.cavallium.dbengine.lucene.searcher.CurrentPageInfo.EMPTY_STATUS;
import static it.cavallium.dbengine.lucene.searcher.CurrentPageInfo.TIE_BREAKER;

import it.cavallium.dbengine.database.LLKeyScore;
import it.cavallium.dbengine.lucene.LuceneUtils;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopFieldDocs;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.Empty;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

class ScoredSimpleLuceneShardSearcher implements LuceneShardSearcher {

	private final Object lock = new Object();
	private final List<IndexSearcher> indexSearchersArray = new ArrayList<>();
	private final List<Mono<Void>> indexSearcherReleasersArray = new ArrayList<>();
	private final List<TopFieldCollector> collectors = new ArrayList<>();
	private final CollectorManager<TopFieldCollector, TopDocs> firstPageSharedManager;
	private final Query luceneQuery;
	private final PaginationInfo paginationInfo;

	public ScoredSimpleLuceneShardSearcher(CollectorManager<TopFieldCollector, TopDocs> firstPageSharedManager,
			Query luceneQuery, PaginationInfo paginationInfo) {
		this.firstPageSharedManager = firstPageSharedManager;
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
			TopFieldCollector collector;
			synchronized (lock) {
				//noinspection BlockingMethodInNonBlockingContext
				collector = firstPageSharedManager.newCollector();
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
	public Mono<LuceneSearchResult> collect(LocalQueryParams queryParams, String keyFieldName, Scheduler collectorScheduler) {
		if (Schedulers.isInNonBlockingThread()) {
			return Mono.error(() -> new UnsupportedOperationException("Called collect in a nonblocking thread"));
		}
		if (!queryParams.isScored()) {
			return Mono.error(() -> new UnsupportedOperationException("Can't execute an unscored query"
					+ " with a scored lucene shard searcher"));
		}
		return Mono
				.fromCallable(() -> {
					TopDocs result;
					Mono<Void> release;
					synchronized (lock) {
						//noinspection BlockingMethodInNonBlockingContext
						result = firstPageSharedManager.reduce(collectors);
						release = Mono.when(indexSearcherReleasersArray);
					}
					IndexSearchers indexSearchers;
					synchronized (lock) {
						indexSearchers = IndexSearchers.of(indexSearchersArray);
					}
					Flux<LLKeyScore> firstPageHits = LuceneUtils
							.convertHits(Flux.fromArray(result.scoreDocs), indexSearchers, keyFieldName, collectorScheduler, true);

					Flux<LLKeyScore> nextHits;
					nextHits = Flux
							.<TopDocs, CurrentPageInfo>generate(
									() -> new CurrentPageInfo(LuceneUtils.getLastFieldDoc(result.scoreDocs),
											paginationInfo.totalLimit() - paginationInfo.firstPageLimit(), 1),
									(s, emitter) -> {
										if (Schedulers.isInNonBlockingThread()) {
											throw new UnsupportedOperationException("Called collect in a nonblocking thread");
										}

										if (s.last() != null && s.remainingLimit() > 0) {
											Sort luceneSort = queryParams.sort();
											if (luceneSort == null) {
												luceneSort = Sort.RELEVANCE;
											}
											CollectorManager<TopFieldCollector, TopDocs> sharedManager
													= new ScoringShardsCollectorManager(luceneSort, s.currentPageLimit(),
													(FieldDoc) s.last(), LuceneUtils.totalHitsThreshold(), 0, s.currentPageLimit());

											try {
												var collectors = new ObjectArrayList<TopFieldCollector>(indexSearchersArray.size());
												for (IndexSearcher indexSearcher : indexSearchersArray) {
													//noinspection BlockingMethodInNonBlockingContext
													TopFieldCollector collector = sharedManager.newCollector();
													//noinspection BlockingMethodInNonBlockingContext
													indexSearcher.search(luceneQuery, collector);

													collectors.add(collector);
												}

												//noinspection BlockingMethodInNonBlockingContext
												var pageTopDocs = sharedManager.reduce(collectors);
												var pageLastDoc = LuceneUtils.getLastFieldDoc(pageTopDocs.scoreDocs);
												emitter.next(pageTopDocs);

												s = new CurrentPageInfo(pageLastDoc, s.remainingLimit() - s.currentPageLimit(),
														s.pageIndex() + 1);
											} catch (IOException ex) {
												emitter.error(ex);
												s = EMPTY_STATUS;
											}
										} else {
											emitter.complete();
											s = EMPTY_STATUS;
										}
										return s;
							})
							.subscribeOn(collectorScheduler)
							.transform(flux -> {
								if (paginationInfo.forceSinglePage()
										|| paginationInfo.totalLimit() - paginationInfo.firstPageLimit() <= 0) {
									return Flux.empty();
								} else {
									return flux;
								}
							})
							.flatMapSequential(topFieldDoc -> LuceneUtils
									.convertHits(Flux.fromArray(topFieldDoc.scoreDocs), indexSearchers,
											keyFieldName, collectorScheduler, true),
									2
							);

					return new LuceneSearchResult(LuceneUtils.convertTotalHitsCount(result.totalHits),
							firstPageHits
									.concatWith(nextHits),
									//.transform(flux -> LuceneUtils.filterTopDoc(flux, queryParams)),
							release
					);
				})
				.subscribeOn(collectorScheduler);
	}

}
