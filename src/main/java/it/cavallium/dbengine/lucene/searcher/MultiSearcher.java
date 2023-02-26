package it.cavallium.dbengine.lucene.searcher;

import it.cavallium.dbengine.database.LLKeyScore;
import it.cavallium.dbengine.database.disk.LLIndexSearcher;
import it.cavallium.dbengine.database.disk.LLIndexSearchers;
import java.io.IOException;
import java.util.function.Function;
import java.util.stream.Stream;
import org.jetbrains.annotations.Nullable;

public interface MultiSearcher extends LocalSearcher {

	/**
	 * @param indexSearchers Lucene index searcher
	 * @param queryParams        the query parameters
	 * @param keyFieldName       the name of the key field
	 * @param transformer        the search query transformer
	 * @param filterer          the search result filterer
	 */
	LuceneSearchResult collectMulti(LLIndexSearchers indexSearchers,
			LocalQueryParams queryParams,
			@Nullable String keyFieldName,
			GlobalQueryRewrite transformer,
			Function<Stream<LLKeyScore>, Stream<LLKeyScore>> filterer);

	/**
	 * @param indexSearcher Lucene index searcher
	 * @param queryParams       the query parameters
	 * @param keyFieldName      the name of the key field
	 * @param transformer       the search query transformer
	 * @param filterer          the search result filterer
	 */
	@Override
	default LuceneSearchResult collect(LLIndexSearcher indexSearcher,
			LocalQueryParams queryParams,
			@Nullable String keyFieldName,
			GlobalQueryRewrite transformer,
			Function<Stream<LLKeyScore>, Stream<LLKeyScore>> filterer) {
		LLIndexSearchers searchers = LLIndexSearchers.unsharded(indexSearcher);
		return this.collectMulti(searchers, queryParams, keyFieldName, transformer, filterer);
	}

}
