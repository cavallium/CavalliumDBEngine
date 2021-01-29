package it.cavallium.dbengine.database.luceneutil;

import it.cavallium.dbengine.database.LLKeyScore;
import java.io.IOException;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.jetbrains.annotations.Nullable;

/**
 * Search that only count approximate results without returning any result
 */
public class CountStreamSearcher implements LuceneStreamSearcher {

	@Override
	public void search(IndexSearcher indexSearcher,
			Query query,
			int limit,
			@Nullable Sort luceneSort,
			ScoreMode scoreMode,
			String keyFieldName,
			Consumer<LLKeyScore> resultsConsumer,
			LongConsumer totalHitsConsumer) throws IOException {
		if (limit != 0) {
			throw new IllegalArgumentException("CountStream doesn't support a limit different than 0");
		}
		if (luceneSort != null) {
			throw new IllegalArgumentException("CountStream doesn't support sorting");
		}
		if (resultsConsumer != null) {
			throw new IllegalArgumentException("CountStream doesn't support a results consumer");
		}
		if (keyFieldName != null) {
			throw new IllegalArgumentException("CountStream doesn't support a key field");
		}
		totalHitsConsumer.accept(count(indexSearcher, query));
	}

	public long count(IndexSearcher indexSearcher, Query query) throws IOException {
		return indexSearcher.count(query);
	}
}
