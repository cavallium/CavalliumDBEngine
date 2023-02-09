package it.cavallium.dbengine.lucene.searcher;

import it.cavallium.dbengine.database.disk.LLIndexSearcher;
import it.cavallium.dbengine.database.disk.LLIndexSearchers;
import java.io.IOException;
import org.jetbrains.annotations.Nullable;

public interface MultiSearcher extends LocalSearcher {

	/**
	 * @param indexSearchersMono Lucene index searcher
	 * @param queryParams        the query parameters
	 * @param keyFieldName       the name of the key field
	 * @param transformer        the search query transformer
	 */
	LuceneSearchResult collectMulti(LLIndexSearchers indexSearchersMono,
			LocalQueryParams queryParams,
			@Nullable String keyFieldName,
			GlobalQueryRewrite transformer);

	/**
	 * @param indexSearcher Lucene index searcher
	 * @param queryParams       the query parameters
	 * @param keyFieldName      the name of the key field
	 * @param transformer       the search query transformer
	 */
	@Override
	default LuceneSearchResult collect(LLIndexSearcher indexSearcher,
			LocalQueryParams queryParams,
			@Nullable String keyFieldName,
			GlobalQueryRewrite transformer) {
		LLIndexSearchers searchers = LLIndexSearchers.unsharded(indexSearcher);
		return this.collectMulti(searchers, queryParams, keyFieldName, transformer);
	}

}
