package it.cavallium.dbengine.lucene.searcher;

import it.cavallium.dbengine.database.LLKeyScore;
import it.cavallium.dbengine.lucene.LuceneParallelStreamCollectorManager;
import it.cavallium.dbengine.lucene.LuceneParallelStreamCollectorResult;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
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

	private final CountStreamSearcher countStreamSearcher;

	public ParallelCollectorStreamSearcher(CountStreamSearcher countStreamSearcher) {
		this.countStreamSearcher = countStreamSearcher;
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
		if (offset != 0) {
			throw new IllegalArgumentException("ParallelCollectorStreamSearcher doesn't support a offset different than 0");
		}
		if (luceneSort != null) {
			throw new IllegalArgumentException("ParallelCollectorStreamSearcher doesn't support sorted searches");
		}

		return new LuceneSearchInstance() {

			long totalHitsCount = countStreamSearcher.countLong(indexSearcher, query);

			@Override
			public long getTotalHitsCount() throws IOException {
				return totalHitsCount;
			}

			@Override
			public void getResults(ResultItemConsumer resultsConsumer) throws IOException {
				AtomicInteger currentCount = new AtomicInteger();

				LuceneParallelStreamCollectorResult result = indexSearcher.search(query,
						LuceneParallelStreamCollectorManager.fromConsumer(scoreMode, minCompetitiveScore, (docId, score) -> {
					if (currentCount.getAndIncrement() >= limit) {
						return HandleResult.HALT;
					} else {
						Document d = indexSearcher.doc(docId, Set.of(keyFieldName));
						if (d.getFields().isEmpty()) {
							logger.error("The document docId: {} is empty.", docId);
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
								logger.error("Can't get key of document docId: {}", docId);
							} else {
								if (resultsConsumer.accept(new LLKeyScore(field.stringValue(), score)) == HandleResult.HALT) {
									return HandleResult.HALT;
								}
							}
						}
						return HandleResult.CONTINUE;
					}
				}));
				this.totalHitsCount = result.getTotalHitsCount();
			}
		};
	}
}
