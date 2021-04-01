package it.cavallium.dbengine.lucene.searcher;

import it.cavallium.dbengine.lucene.LuceneUtils;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.jetbrains.annotations.Nullable;
import org.warp.commonutils.type.IntWrapper;

/**
 * Sorted paged search (the most memory-efficient stream searcher for big queries)
 */
public class PagedStreamSearcher implements LuceneStreamSearcher {

	public static final int MAX_ITEMS_PER_PAGE = 1000;
	private final SimpleStreamSearcher simpleStreamSearcher;

	public PagedStreamSearcher(SimpleStreamSearcher simpleStreamSearcher) {
		this.simpleStreamSearcher = simpleStreamSearcher;
	}

	@Override
	public LuceneSearchInstance search(IndexSearcher indexSearcher,
			Query query,
			int offset,
			int limit,
			@Nullable Sort luceneSort,
			ScoreMode scoreMode,
			@Nullable Float minCompetitiveScore,
			String keyFieldName) throws IOException {
		if (limit < MAX_ITEMS_PER_PAGE) {
			// Use a normal search method because the limit is low
			simpleStreamSearcher.search(indexSearcher,
					query,
					offset,
					limit,
					luceneSort,
					scoreMode,
					minCompetitiveScore,
					keyFieldName
			);
		}

		IntWrapper currentAllowedResults = new IntWrapper(limit);

		// Run the first page search
		TopDocs firstTopDocsVal;
		if (offset == 0) {
			firstTopDocsVal = indexSearcher.search(query,
					MAX_ITEMS_PER_PAGE,
					luceneSort,
					scoreMode != ScoreMode.COMPLETE_NO_SCORES
			);
		} else {
			firstTopDocsVal = new TopDocsSearcher(indexSearcher,
					query,
					luceneSort,
					MAX_ITEMS_PER_PAGE,
					null,
					scoreMode != ScoreMode.COMPLETE_NO_SCORES,
					1000
			).getTopDocs(offset, MAX_ITEMS_PER_PAGE);
		}
		AtomicReference<TopDocs> firstTopDocs = new AtomicReference<>(firstTopDocsVal);
		long totalHitsCount = firstTopDocs.getPlain().totalHits.value;

		return new LuceneSearchInstance() {
			@Override
			public long getTotalHitsCount() {
				return totalHitsCount;
			}

			@Override
			public void getResults(ResultItemConsumer resultsConsumer) throws IOException {
				TopDocs lastTopDocs = firstTopDocs.getAndSet(null);
				if (lastTopDocs.scoreDocs.length > 0) {
					ScoreDoc lastScoreDoc = getLastItem(lastTopDocs.scoreDocs);
					consumeHits(currentAllowedResults,
							lastTopDocs.scoreDocs,
							indexSearcher,
							minCompetitiveScore,
							keyFieldName,
							resultsConsumer
					);

					// Run the searches for each page until the end
					boolean finished = currentAllowedResults.var <= 0;
					while (!finished) {
						boolean halted;
						lastTopDocs = indexSearcher.searchAfter(lastScoreDoc,
								query,
								MAX_ITEMS_PER_PAGE,
								luceneSort,
								scoreMode != ScoreMode.COMPLETE_NO_SCORES
						);
						if (lastTopDocs.scoreDocs.length > 0) {
							lastScoreDoc = getLastItem(lastTopDocs.scoreDocs);
							halted = consumeHits(currentAllowedResults,
									lastTopDocs.scoreDocs,
									indexSearcher,
									minCompetitiveScore,
									keyFieldName,
									resultsConsumer
							) == HandleResult.HALT;
						} else {
							halted = false;
						}
						if (lastTopDocs.scoreDocs.length < MAX_ITEMS_PER_PAGE || currentAllowedResults.var <= 0 || halted) {
							finished = true;
						}
					}
				}
			}
		};
	}

	private HandleResult consumeHits(IntWrapper currentAllowedResults,
			ScoreDoc[] hits,
			IndexSearcher indexSearcher,
			@Nullable Float minCompetitiveScore,
			String keyFieldName,
			ResultItemConsumer resultsConsumer) throws IOException {
		for (ScoreDoc hit : hits) {
			int docId = hit.doc;
			float score = hit.score;

			if (currentAllowedResults.var-- > 0) {
				if (LuceneUtils.collectTopDoc(logger,
						docId,
						score,
						minCompetitiveScore,
						indexSearcher,
						keyFieldName,
						resultsConsumer
				) == HandleResult.HALT) {
					return HandleResult.HALT;
				}
			} else {
				break;
			}
		}
		return HandleResult.CONTINUE;
	}

	private static ScoreDoc getLastItem(ScoreDoc[] scoreDocs) {
		return scoreDocs[scoreDocs.length - 1];
	}

}
