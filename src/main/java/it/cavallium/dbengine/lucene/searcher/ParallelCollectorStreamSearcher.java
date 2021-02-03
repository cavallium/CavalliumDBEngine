package it.cavallium.dbengine.lucene.searcher;

import it.cavallium.dbengine.database.LLKeyScore;
import it.cavallium.dbengine.lucene.LuceneParallelStreamCollectorManager;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.jetbrains.annotations.Nullable;

/**
 * Unsorted search (low latency and constant memory usage)
 */
public class ParallelCollectorStreamSearcher implements LuceneStreamSearcher {

	@Override
	public void search(IndexSearcher indexSearcher,
			Query query,
			int limit,
			@Nullable Sort luceneSort,
			ScoreMode scoreMode,
			String keyFieldName,
			Consumer<LLKeyScore> resultsConsumer,
			LongConsumer totalHitsConsumer) throws IOException {
		if (luceneSort != null) {
			throw new IllegalArgumentException("ParallelCollectorStreamSearcher doesn't support sorted searches");
		}

		AtomicInteger currentCount = new AtomicInteger();

		var result = indexSearcher.search(query, LuceneParallelStreamCollectorManager.fromConsumer(scoreMode, (docId, score) -> {
			if (currentCount.getAndIncrement() >= limit) {
				return false;
			} else {
				try {
					Document d = indexSearcher.doc(docId, Set.of(keyFieldName));
					if (d.getFields().isEmpty()) {
						logger.error("The document docId: {} is empty.", docId);
						var realFields = indexSearcher.doc(docId).getFields();
						if (!realFields.isEmpty()) {
							logger.error("Present fields:");
							for (IndexableField field : realFields) {
								logger.error(" - " + field.name());
							}
						}
					} else {
						var field = d.getField(keyFieldName);
						if (field == null) {
							logger.error("Can't get key of document docId:" + docId);
						} else {
							resultsConsumer.accept(new LLKeyScore(field.stringValue(), score));
						}
					}
				} catch (IOException e) {
					e.printStackTrace();
					throw new CompletionException(e);
				}
				return true;
			}
		}));
		//todo: check the accuracy of our hits counter!
		totalHitsConsumer.accept(result.getTotalHitsCount());
	}
}
