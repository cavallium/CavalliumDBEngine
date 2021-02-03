package it.cavallium.dbengine.lucene.searcher;

import it.cavallium.dbengine.database.LLKeyScore;
import java.io.IOException;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
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
	private final LuceneStreamSearcher baseStreamSearcher;

	public PagedStreamSearcher(LuceneStreamSearcher baseStreamSearcher) {
		this.baseStreamSearcher = baseStreamSearcher;
	}

	@Override
	public void search(IndexSearcher indexSearcher,
			Query query,
			int limit,
			@Nullable Sort luceneSort,
			ScoreMode scoreMode,
			String keyFieldName,
			Consumer<LLKeyScore> resultsConsumer,
			LongConsumer totalHitsConsumer) throws IOException {
		if (limit < MAX_ITEMS_PER_PAGE) {
			// Use a normal search method because the limit is low
			baseStreamSearcher.search(indexSearcher, query, limit, luceneSort, scoreMode, keyFieldName, resultsConsumer, totalHitsConsumer);
			return;
		}
		IntWrapper currentAllowedResults = new IntWrapper(limit);

		// Run the first page search
		TopDocs lastTopDocs = indexSearcher.search(query, MAX_ITEMS_PER_PAGE, luceneSort, scoreMode != ScoreMode.COMPLETE_NO_SCORES);
		totalHitsConsumer.accept(lastTopDocs.totalHits.value);
		if (lastTopDocs.scoreDocs.length > 0) {
			ScoreDoc lastScoreDoc = getLastItem(lastTopDocs.scoreDocs);
			consumeHits(currentAllowedResults, lastTopDocs.scoreDocs, indexSearcher, keyFieldName, resultsConsumer);

			// Run the searches for each page until the end
			boolean finished = currentAllowedResults.var <= 0;
			while (!finished) {
				lastTopDocs = indexSearcher.searchAfter(lastScoreDoc, query, MAX_ITEMS_PER_PAGE, luceneSort, scoreMode != ScoreMode.COMPLETE_NO_SCORES);
				if (lastTopDocs.scoreDocs.length > 0) {
					lastScoreDoc = getLastItem(lastTopDocs.scoreDocs);
					consumeHits(currentAllowedResults, lastTopDocs.scoreDocs, indexSearcher, keyFieldName, resultsConsumer);
				}
				if (lastTopDocs.scoreDocs.length < MAX_ITEMS_PER_PAGE || currentAllowedResults.var <= 0) {
					finished = true;
				}
			}
		}
	}

	private void consumeHits(IntWrapper currentAllowedResults,
			ScoreDoc[] hits,
			IndexSearcher indexSearcher,
			String keyFieldName,
			Consumer<LLKeyScore> resultsConsumer) throws IOException {
		for (ScoreDoc hit : hits) {
			int docId = hit.doc;
			float score = hit.score;

			if (currentAllowedResults.var-- > 0) {
				Document d = indexSearcher.doc(docId, Set.of(keyFieldName));
				if (d.getFields().isEmpty()) {
					logger.error("The document docId: {}, score: {} is empty.", docId, score);
					var realFields = indexSearcher.doc(docId).getFields();
					if (!realFields.isEmpty()) {
						logger.error("Present fields:");
						for (IndexableField field : realFields) {
							logger.error(" - {}", field.name());
						}
					}
				} else {
					var field = d.getField(keyFieldName);
					if (field == null) {
						logger.error("Can't get key of document docId: {}, score: {}", docId, score);
					} else {
						resultsConsumer.accept(new LLKeyScore(field.stringValue(), score));
					}
				}
			} else {
				break;
			}
		}
	}

	private static ScoreDoc getLastItem(ScoreDoc[] scoreDocs) {
		return scoreDocs[scoreDocs.length - 1];
	}
}
