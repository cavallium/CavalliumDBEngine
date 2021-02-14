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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface LuceneStreamSearcher {

	Logger logger = LoggerFactory.getLogger(LuceneStreamSearcher.class);

	/**
	 * Do a lucene query, receiving the single results using a consumer
	 * @param indexSearcher the index searcher, which contains all the lucene data
	 * @param query the query
	 * @param limit the maximum number of results
	 * @param luceneSort the sorting method used for the search
	 * @param scoreMode score mode
	 * @param minCompetitiveScore minimum score accepted
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
			@Nullable Float minCompetitiveScore,
			String keyFieldName,
			Consumer<LLKeyScore> resultsConsumer,
			LongConsumer totalHitsConsumer) throws IOException;
}
