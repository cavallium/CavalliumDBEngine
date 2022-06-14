package it.cavallium.dbengine.lucene.searcher;

import io.netty5.buffer.api.Send;
import it.cavallium.dbengine.database.disk.LLIndexSearcher;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Mono;

public interface LocalSearcher {

	/**
	 * @param indexSearcherMono Lucene index searcher
	 * @param queryParams       the query parameters
	 * @param keyFieldName      the name of the key field
	 * @param transformer       the search query transformer
	 */
	Mono<LuceneSearchResult> collect(Mono<LLIndexSearcher> indexSearcherMono,
			LocalQueryParams queryParams,
			@Nullable String keyFieldName,
			GlobalQueryRewrite transformer);

	/**
	 * Get the name of this searcher type
	 * @return searcher type name
	 */
	String getName();
}
