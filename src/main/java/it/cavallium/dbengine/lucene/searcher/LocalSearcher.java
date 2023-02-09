package it.cavallium.dbengine.lucene.searcher;

import it.cavallium.dbengine.database.disk.LLIndexSearcher;
import org.jetbrains.annotations.Nullable;

public interface LocalSearcher {

	/**
	 * @param indexSearcher Lucene index searcher
	 * @param queryParams       the query parameters
	 * @param keyFieldName      the name of the key field
	 * @param transformer       the search query transformer
	 */
	LuceneSearchResult collect(LLIndexSearcher indexSearcher,
			LocalQueryParams queryParams,
			@Nullable String keyFieldName,
			GlobalQueryRewrite transformer);

	/**
	 * Get the name of this searcher type
	 * @return searcher type name
	 */
	String getName();
}
