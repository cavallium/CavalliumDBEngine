package it.cavallium.dbengine.lucene.searcher;

import it.cavallium.dbengine.database.LLKeyScore;
import it.cavallium.dbengine.lucene.LuceneUtils;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.io.IOException;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.jetbrains.annotations.Nullable;

/**
 * Sorted search (slower and more memory-intensive)
 */
public class SimpleStreamSearcher implements LuceneStreamSearcher {

	@Override
	public void search(IndexSearcher indexSearcher,
			Query query,
			int limit,
			@Nullable Sort luceneSort,
			ScoreMode scoreMode,
			@Nullable Float minCompetitiveScore,
			String keyFieldName,
			Consumer<LLKeyScore> resultsConsumer,
			LongConsumer totalHitsConsumer) throws IOException {
		TopDocs topDocs;
		if (luceneSort != null) {
			topDocs = indexSearcher.search(query, limit, luceneSort, scoreMode != ScoreMode.COMPLETE_NO_SCORES);
		} else {
			topDocs = indexSearcher.search(query, limit);
		}
		totalHitsConsumer.accept(topDocs.totalHits.value);
		var hits = ObjectArrayList.wrap(topDocs.scoreDocs);
		for (ScoreDoc hit : hits) {
			int docId = hit.doc;
			float score = hit.score;
			LuceneUtils.collectTopDoc(logger,
					docId,
					score,
					minCompetitiveScore,
					indexSearcher,
					keyFieldName,
					resultsConsumer
			);
		}
	}
}
