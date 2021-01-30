package it.cavallium.dbengine.lucene.searcher;

import it.cavallium.dbengine.database.LLKeyScore;
import java.io.IOException;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.jetbrains.annotations.Nullable;

public interface LuceneStreamSearcher {

	/**
	 * Do a lucene query, receiving the single results using a consumer
	 * @param indexSearcher the index searcher, which contains all the lucene data
	 * @param query the query
	 * @param limit the maximum number of results
	 * @param luceneSort the sorting method used for the search
	 * @param scoreMode score mode
	 * @param keyFieldName the name of the key field
	 * @param resultsConsumer the consumer of results
	 * @param totalHitsConsumer the consumer of total count of results
	 * @throws IOException thrown if there is an error
	 */
	void search(IndexSearcher indexSearcher,
			Query query,
			int limit,
			@Nullable Sort luceneSort,
			ScoreMode scoreMode,
			String keyFieldName,
			Consumer<LLKeyScore> resultsConsumer,
			LongConsumer totalHitsConsumer) throws IOException;
}
