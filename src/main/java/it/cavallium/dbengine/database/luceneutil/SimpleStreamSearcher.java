package it.cavallium.dbengine.database.luceneutil;

import it.cavallium.dbengine.database.LLKeyScore;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
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
			String keyFieldName,
			Consumer<LLKeyScore> resultsConsumer,
			LongConsumer totalHitsConsumer) throws IOException {
		TopDocs topDocs = indexSearcher.search(query, limit, luceneSort, scoreMode != ScoreMode.COMPLETE_NO_SCORES);
		totalHitsConsumer.accept(topDocs.totalHits.value);
		var hits = ObjectArrayList.wrap(topDocs.scoreDocs);
		for (ScoreDoc hit : hits) {
			int docId = hit.doc;
			float score = hit.score;
			Document d = indexSearcher.doc(docId, Set.of(keyFieldName));
			if (d.getFields().isEmpty()) {
				System.err.println("The document docId:" + docId + ",score:" + score + " is empty.");
				var realFields = indexSearcher.doc(docId).getFields();
				if (!realFields.isEmpty()) {
					System.err.println("Present fields:");
					for (IndexableField field : realFields) {
						System.err.println(" - " + field.name());
					}
				}
			} else {
				var field = d.getField(keyFieldName);
				if (field == null) {
					System.err.println("Can't get key of document docId:" + docId + ",score:" + score);
				} else {
					resultsConsumer.accept(new LLKeyScore(field.stringValue(), score));
				}
			}
		}
	}
}
