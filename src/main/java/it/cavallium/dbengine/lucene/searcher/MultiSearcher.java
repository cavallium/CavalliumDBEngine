package it.cavallium.dbengine.lucene.searcher;

import static it.cavallium.dbengine.database.LLUtils.singleOrClose;

import io.netty5.buffer.api.Send;
import it.cavallium.dbengine.database.disk.LLIndexSearcher;
import it.cavallium.dbengine.database.disk.LLIndexSearchers;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Mono;

public interface MultiSearcher extends LocalSearcher {

	/**
	 * @param indexSearchersMono Lucene index searcher
	 * @param queryParams        the query parameters
	 * @param keyFieldName       the name of the key field
	 * @param transformer        the search query transformer
	 */
	Mono<LuceneSearchResult> collectMulti(Mono<LLIndexSearchers> indexSearchersMono,
			LocalQueryParams queryParams,
			@Nullable String keyFieldName,
			GlobalQueryRewrite transformer);

	/**
	 * @param indexSearcherMono Lucene index searcher
	 * @param queryParams       the query parameters
	 * @param keyFieldName      the name of the key field
	 * @param transformer       the search query transformer
	 */
	@Override
	default Mono<LuceneSearchResult> collect(Mono<LLIndexSearcher> indexSearcherMono,
			LocalQueryParams queryParams,
			@Nullable String keyFieldName,
			GlobalQueryRewrite transformer) {
		Mono<LLIndexSearchers> searchers = indexSearcherMono.map(indexSearcher -> LLIndexSearchers.unsharded(indexSearcher));
		return this.collectMulti(searchers, queryParams, keyFieldName, transformer);
	}

}
