package it.cavallium.dbengine.lucene.searcher;

import static it.cavallium.dbengine.lucene.searcher.CurrentPageInfo.EMPTY_STATUS;

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

	public UnscoredPagedLuceneShardSearcher(CollectorManager<TopDocsCollector<ScoreDoc>, TopDocs> unsortedCollectorManager,
			Query luceneQuery,
			PaginationInfo paginationInfo) {
		this.firstPageUnsortedCollectorManager = unsortedCollectorManager;
		this.luceneQuery = luceneQuery;
		this.paginationInfo = paginationInfo;
	}

	@Override
	public Mono<Void> searchOn(IndexSearcher indexSearcher,
			Mono<Void> releaseIndexSearcher,
			LocalQueryParams queryParams,
			Scheduler scheduler) {
		return Mono.fromCallable(() -> {
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
							.convertHits(result.scoreDocs, indexSearchers, keyFieldName, scheduler, false);

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
												UnscoredTopDocsCollectorManager currentPageUnsortedCollectorManager = new UnscoredTopDocsCollectorManager(
														() -> TopDocsSearcher.getTopDocsCollector(queryParams.sort(), s.currentPageLimit(),
																s.last(), LuceneUtils.totalHitsThreshold(), true, queryParams.isScored()),
														0, s.currentPageLimit(), queryParams.sort());
												//noinspection BlockingMethodInNonBlockingContext
												TopDocs pageTopDocs = Flux
														.fromIterable(indexSearchersArray)
														.flatMapSequential(indexSearcher -> Mono
																.fromCallable(() -> {
																	//noinspection BlockingMethodInNonBlockingContext
																	var collector = currentPageUnsortedCollectorManager.newCollector();
																	//noinspection BlockingMethodInNonBlockingContext
																	indexSearcher.search(luceneQuery, collector);
																	return collector;
																})
																.subscribeOn(scheduler)
														)
														.collect(Collectors.toCollection(ObjectArrayList::new))
														.flatMap(collectors -> Mono
																.fromCallable(() -> currentPageUnsortedCollectorManager.reduce(collectors))
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
								.flatMapSequential(topFieldDoc -> LuceneUtils
										.convertHits(topFieldDoc.scoreDocs, indexSearchers, keyFieldName, scheduler, false)
								);
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
