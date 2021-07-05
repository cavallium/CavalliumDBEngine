package it.cavallium.dbengine.lucene.searcher;

import it.cavallium.dbengine.database.LLKeyScore;
import it.cavallium.dbengine.lucene.LuceneUtils;
import it.cavallium.dbengine.lucene.searcher.LuceneStreamSearcher.HandleResult;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

public class PagedLuceneReactiveSearcher implements LuceneReactiveSearcher {

	private static final int FIRST_PAGE_HITS_MAX_COUNT = 1;
	private static final long MIN_HITS_PER_PAGE = 5;
	private static final long MAX_HITS_PER_PAGE = 100;

	@SuppressWarnings("BlockingMethodInNonBlockingContext")
	@Override
	public Mono<LuceneReactiveSearchInstance> search(IndexSearcher indexSearcher,
			Query query,
			int offset,
			int limit,
			@Nullable Sort luceneSort,
			ScoreMode scoreMode,
			@Nullable Float minCompetitiveScore,
			String keyFieldName,
			Scheduler scheduler) {
		return Mono
				.fromCallable(() -> {
					// Run the first page search
					TopDocs firstTopDocsVal;
					if (offset == 0) {
						firstTopDocsVal = TopDocsSearcher.getTopDocs(indexSearcher,
								query,
								luceneSort,
								FIRST_PAGE_HITS_MAX_COUNT,
								null,
								scoreMode != ScoreMode.COMPLETE_NO_SCORES,
								1000
						);
					} else {
						firstTopDocsVal = TopDocsSearcher.getTopDocs(indexSearcher,
								query,
								luceneSort,
								offset + FIRST_PAGE_HITS_MAX_COUNT,
								null,
								scoreMode != ScoreMode.COMPLETE_NO_SCORES,
								1000,
								offset, FIRST_PAGE_HITS_MAX_COUNT);
					}
					long totalHitsCount = firstTopDocsVal.totalHits.value;
					Flux<LLKeyScore> firstPageHitsFlux =  LuceneReactiveSearcher.convertHits(
							firstTopDocsVal.scoreDocs,
							indexSearcher,
							minCompetitiveScore,
							keyFieldName,
							scheduler
					);

					Flux<LLKeyScore> nextPagesFlux = Flux
							.<Flux<LLKeyScore>, PageState>generate(
									() -> new PageState(getLastItem(firstTopDocsVal.scoreDocs), 0),
									(s, sink) -> {
										if (s.lastItem() == null) {
											sink.complete();
											return new PageState(null, 0);
										}

										try {
											var lastTopDocs = TopDocsSearcher.getTopDocs(indexSearcher,
													query,
													luceneSort,
													s.hitsPerPage(),
													s.lastItem(),
													scoreMode != ScoreMode.COMPLETE_NO_SCORES,
													1000
											);
											if (lastTopDocs.scoreDocs.length > 0) {
												ScoreDoc lastItem = getLastItem(lastTopDocs.scoreDocs);
												var hitsList = LuceneReactiveSearcher.convertHits(
														lastTopDocs.scoreDocs,
														indexSearcher,
														minCompetitiveScore,
														keyFieldName,
														scheduler
												);
												sink.next(hitsList);
												return new PageState(lastItem, s.currentPageIndex() + 1);
											} else {
												sink.complete();
												return new PageState(null, 0);
											}
										} catch (IOException e) {
											sink.error(e);
											return new PageState(null, 0);
										}
									}
							)
							.subscribeOn(scheduler)
							.concatMap(Flux::hide);

					Flux<LLKeyScore> resultsFlux = firstPageHitsFlux
							.concatWith(nextPagesFlux)
							.take(limit, true);


					if (limit == 0) {
						return new LuceneReactiveSearchInstance(totalHitsCount, Flux.empty());
					} else {
						return new LuceneReactiveSearchInstance(totalHitsCount, resultsFlux);
					}
				})
				.subscribeOn(scheduler);
	}

	private static ScoreDoc getLastItem(ScoreDoc[] scoreDocs) {
		if (scoreDocs.length == 0) {
			return null;
		}
		return scoreDocs[scoreDocs.length - 1];
	}

	private record PageState(ScoreDoc lastItem, int currentPageIndex) {

		public int hitsPerPage() {
			return (int) Math.min(MAX_HITS_PER_PAGE, MIN_HITS_PER_PAGE * (1L << currentPageIndex));
		}
	}
}
