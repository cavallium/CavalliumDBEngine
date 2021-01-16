package it.cavallium.dbengine.database.luceneutil;

import java.io.IOException;
import java.util.function.Consumer;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.jetbrains.annotations.Nullable;

/**
 * Search that only count approximate results without returning any result
 */
public class CountStreamSearcher implements LuceneStreamSearcher {

	@Override
	public Long streamSearch(IndexSearcher indexSearcher,
			Query query,
			int limit,
			@Nullable Sort luceneSort,
			String keyFieldName,
			Consumer<String> consumer) throws IOException {
		if (limit != 0) {
			throw new IllegalArgumentException("CountStream doesn't support a limit different than 0");
		}
		if (luceneSort != null) {
			throw new IllegalArgumentException("CountStream doesn't support sorting");
		}
		if (consumer != null) {
			throw new IllegalArgumentException("CountStream doesn't support a results consumer");
		}
		if (keyFieldName != null) {
			throw new IllegalArgumentException("CountStream doesn't support a key field");
		}
		return count(indexSearcher, query);
	}

	public long count(IndexSearcher indexSearcher, Query query) throws IOException {
		return indexSearcher.count(query);
	}
}
