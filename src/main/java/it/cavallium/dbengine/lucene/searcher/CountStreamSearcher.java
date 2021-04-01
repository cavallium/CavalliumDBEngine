package it.cavallium.dbengine.lucene.searcher;

import java.io.IOException;
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
	public LuceneSearchInstance search(IndexSearcher indexSearcher,
			Query query,
			int offset,
			int limit,
			@Nullable Sort luceneSort,
			ScoreMode scoreMode,
			@Nullable Float minCompetitiveScore,
			String keyFieldName) throws IOException {
		if (limit != 0) {
			throw new IllegalArgumentException("CountStream doesn't support a limit different than 0");
		}
		if (luceneSort != null) {
			throw new IllegalArgumentException("CountStream doesn't support sorting");
		}
		if (keyFieldName != null) {
			throw new IllegalArgumentException("CountStream doesn't support a key field");
		}
		return count(indexSearcher, query);
	}

	public long countLong(IndexSearcher indexSearcher, Query query) throws IOException {
		return indexSearcher.count(query);
	}

	public LuceneSearchInstance count(IndexSearcher indexSearcher, Query query) throws IOException {
		long totalHitsCount = countLong(indexSearcher, query);
		return new LuceneSearchInstance() {
			@Override
			public long getTotalHitsCount() {
				return totalHitsCount;
			}

			@Override
			public void getResults(ResultItemConsumer consumer) {

			}
		};
	}
}
