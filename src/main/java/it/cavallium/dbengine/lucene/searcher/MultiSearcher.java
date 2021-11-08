package it.cavallium.dbengine.lucene.searcher;

import io.net5.buffer.api.Send;
import it.cavallium.dbengine.database.disk.LLIndexSearcher;
import it.cavallium.dbengine.database.disk.LLIndexSearchers;
import reactor.core.publisher.Mono;

public interface MultiSearcher extends LocalSearcher {

	/**
	 * Use in-memory collectors if the expected results count is lower or equal than this limit
	 */
	int MAX_IN_MEMORY_SIZE = 8192;

	/**
	 * @param indexSearchersMono Lucene index searcher
	 * @param queryParams        the query parameters
	 * @param keyFieldName       the name of the key field
	 * @param transformer        the search query transformer
	 */
	Mono<LuceneSearchResult> collectMulti(Mono<Send<LLIndexSearchers>> indexSearchersMono,
			LocalQueryParams queryParams,
			String keyFieldName,
			LLSearchTransformer transformer);

	/**
	 * @param indexSearcherMono Lucene index searcher
	 * @param queryParams       the query parameters
	 * @param keyFieldName      the name of the key field
	 * @param transformer       the search query transformer
	 */
	@Override
	default Mono<LuceneSearchResult> collect(Mono<Send<LLIndexSearcher>> indexSearcherMono,
			LocalQueryParams queryParams,
			String keyFieldName,
			LLSearchTransformer transformer) {
		var searchers = indexSearcherMono.map(a -> LLIndexSearchers.unsharded(a).send());
		return this.collectMulti(searchers, queryParams, keyFieldName, transformer);
	}

}
