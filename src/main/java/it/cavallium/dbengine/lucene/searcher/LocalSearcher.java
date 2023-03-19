package it.cavallium.dbengine.lucene.searcher;

import it.cavallium.dbengine.database.LLKeyScore;
import it.cavallium.dbengine.database.disk.LLIndexSearcher;
import java.util.function.Function;
import java.util.stream.Stream;
import org.jetbrains.annotations.Nullable;

public interface LocalSearcher {

	/**
	 * @param indexSearcher Lucene index searcher
	 * @param queryParams       the query parameters
	 * @param keyFieldName      the name of the key field
	 * @param transformer       the search query transformer
	 * @param filterer          the search result filterer
	 */
	LuceneSearchResult collect(LLIndexSearcher indexSearcher,
			LocalQueryParams queryParams,
			@Nullable String keyFieldName,
			GlobalQueryRewrite transformer,
			Function<Stream<LLKeyScore>, Stream<LLKeyScore>> filterer);

	/**
	 * Get the name of this searcher type
	 * @return searcher type name
	 */
	@Override
	String toString();
}
