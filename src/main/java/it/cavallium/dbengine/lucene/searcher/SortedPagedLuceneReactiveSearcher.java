package it.cavallium.dbengine.lucene.searcher;

import it.cavallium.dbengine.database.LLKeyScore;
import it.cavallium.dbengine.lucene.LuceneUtils;
import it.cavallium.dbengine.lucene.searcher.LuceneStreamSearcher.HandleResult;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

public class SortedPagedLuceneReactiveSearcher implements LuceneReactiveSearcher {

	private static final int FIRST_PAGE_HITS_MAX_COUNT = 10;
	private static final long MIN_HITS_PER_PAGE = 20;
	private static final long MAX_HITS_PER_PAGE = 1000;

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
		if (luceneSort == null) {
			return Mono.error(new IllegalArgumentException("Can't execute unsorted queries"));
		}
		// todo: check if offset and limit play well together.
		//   check especially these cases:
		//    - offset > limit
		//    - offset > FIRST_PAGE_HITS_MAX_COUNT
		//    - offset > MAX_HITS_PER_PAGE
		return Mono
				.fromCallable(() -> {
					// Run the first page (max 1 item) search
					TopDocs firstTopDocsVal;
					if (offset == 0) {
						firstTopDocsVal = indexSearcher.search(query,
								FIRST_PAGE_HITS_MAX_COUNT,
								luceneSort,
								scoreMode != ScoreMode.COMPLETE_NO_SCORES
						);
					} else {
						firstTopDocsVal = TopDocsSearcher.getTopDocs(indexSearcher,
								query,
								luceneSort,
								FIRST_PAGE_HITS_MAX_COUNT,
								null,
								scoreMode != ScoreMode.COMPLETE_NO_SCORES,
								1000,
								offset, FIRST_PAGE_HITS_MAX_COUNT);
					}
					long totalHitsCount = firstTopDocsVal.totalHits.value;
					Mono<List<LLKeyScore>> firstPageHitsMono = Mono
							.fromCallable(() -> convertHits(FIRST_PAGE_HITS_MAX_COUNT, firstTopDocsVal.scoreDocs, indexSearcher, minCompetitiveScore, keyFieldName))
							.single();
					Flux<LLKeyScore> resultsFlux = firstPageHitsMono.flatMapMany(firstPageHits -> {
						int firstPageHitsCount = firstPageHits.size();
						Flux<LLKeyScore> firstPageHitsFlux = Flux.fromIterable(firstPageHits);
						if (firstPageHitsCount < FIRST_PAGE_HITS_MAX_COUNT) {
							return Flux.fromIterable(firstPageHits);
						} else {
							Flux<LLKeyScore> nextPagesFlux = Flux
									.<List<LLKeyScore>, PageState>generate(
											() -> new PageState(getLastItem(firstTopDocsVal.scoreDocs), 0, limit - firstPageHitsCount),
											(s, sink) -> {
												if (s.lastItem() == null || s.remainingLimit() <= 0) {
													sink.complete();
													return new PageState(null, 0,0);
												}

												try {
													var lastTopDocs = indexSearcher.searchAfter(s.lastItem(),
															query,
															s.hitsPerPage(),
															luceneSort,
															scoreMode != ScoreMode.COMPLETE_NO_SCORES
													);
													if (lastTopDocs.scoreDocs.length > 0) {
														ScoreDoc lastItem = getLastItem(lastTopDocs.scoreDocs);
														var hitsList = convertHits(s.remainingLimit(),
																lastTopDocs.scoreDocs,
																indexSearcher,
																minCompetitiveScore,
																keyFieldName
														);
														sink.next(hitsList);
														if (hitsList.size() < s.hitsPerPage()) {
															return new PageState(lastItem, 0, 0);
														} else {
															return new PageState(lastItem, s.currentPageIndex() + 1, s.remainingLimit() - hitsList.size());
														}
													} else {
														sink.complete();
														return new PageState(null, 0, 0);
													}
												} catch (IOException e) {
													sink.error(e);
													return new PageState(null, 0, 0);
												}
											}
									)
									.subscribeOn(scheduler)
									.flatMap(Flux::fromIterable);
							return Flux.concat(firstPageHitsFlux, nextPagesFlux);
						}
					});

					if (limit == 0) {
						return new LuceneReactiveSearchInstance(totalHitsCount, Flux.empty());
					} else {
						return new LuceneReactiveSearchInstance(totalHitsCount, resultsFlux);
					}
				})
				.subscribeOn(scheduler);
	}


	private List<LLKeyScore> convertHits(int currentAllowedResults,
			ScoreDoc[] hits,
			IndexSearcher indexSearcher,
			@Nullable Float minCompetitiveScore,
			String keyFieldName) throws IOException {
		ArrayList<LLKeyScore> collectedResults = new ArrayList<>(hits.length);
		for (ScoreDoc hit : hits) {
			int docId = hit.doc;
			float score = hit.score;

			if (currentAllowedResults-- > 0) {
				@Nullable LLKeyScore collectedDoc = LuceneUtils.collectTopDoc(logger, docId, score,
						minCompetitiveScore, indexSearcher, keyFieldName);
				if (collectedDoc != null) {
					collectedResults.add(collectedDoc);
				}
			} else {
				break;
			}
		}
		return collectedResults;
	}

	private static ScoreDoc getLastItem(ScoreDoc[] scoreDocs) {
		return scoreDocs[scoreDocs.length - 1];
	}

	private record PageState(ScoreDoc lastItem, int currentPageIndex, int remainingLimit) {

		public int hitsPerPage() {
			return (int) Math.min(MAX_HITS_PER_PAGE, MIN_HITS_PER_PAGE * (1L << currentPageIndex));
		}
	}
}
