package it.cavallium.dbengine.lucene.searcher;

import io.net5.buffer.api.Send;
import it.cavallium.dbengine.database.disk.LLIndexSearcher;
import it.cavallium.dbengine.database.disk.LLIndexSearchers;
import reactor.core.publisher.Mono;

public interface MultiSearcher extends LocalSearcher {

	/**
	 * @param indexSearchersMono Lucene index searcher
	 * @param queryParams        the query parameters
	 * @param keyFieldName       the name of the key field
	 * @param transformer        the search query transformer
	 */
	Mono<LuceneSearchResult> collectMulti(Mono<Send<LLIndexSearchers>> indexSearchersMono,
			LocalQueryParams queryParams,
			String keyFieldName,
			GlobalQueryRewrite transformer);

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
			GlobalQueryRewrite transformer) {
		var searchers = indexSearcherMono.map(a -> LLIndexSearchers.unsharded(a).send());
		return this.collectMulti(searchers, queryParams, keyFieldName, transformer);
	}

}
