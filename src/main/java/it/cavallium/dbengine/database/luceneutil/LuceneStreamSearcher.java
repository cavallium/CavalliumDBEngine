package it.cavallium.dbengine.database.luceneutil;

import java.io.IOException;
import java.util.function.Consumer;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.jetbrains.annotations.Nullable;

public interface LuceneStreamSearcher {

	/**
	 * Do a lucene query, receiving the single results using a consumer
	 * @param indexSearcher the index searcher, which contains all the lucene data
	 * @param query the query
	 * @param limit the maximum number of results
	 * @param luceneSort the sorting method used for the search
	 * @param keyFieldName the name of the key field
	 * @param consumer the consumer of results
	 * @return the approximated total count of results
	 * @throws IOException thrown if there is an error
	 */
	Long streamSearch(IndexSearcher indexSearcher,
			Query query,
			int limit,
			@Nullable Sort luceneSort,
			String keyFieldName,
			Consumer<String> consumer) throws IOException;
}
