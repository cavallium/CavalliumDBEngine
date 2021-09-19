package it.cavallium.dbengine.lucene.searcher;

import io.net5.buffer.api.Send;
import it.cavallium.dbengine.database.disk.LLIndexContext;
import it.cavallium.dbengine.database.disk.LLIndexSearcher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface LuceneMultiSearcher extends LuceneLocalSearcher {

	/**
	 * @param queryParams the query parameters
	 * @param keyFieldName the name of the key field
	 */
	Mono<Send<LuceneSearchResult>> collect(Flux<Send<LLIndexContext>> indexSearchersFlux,
			LocalQueryParams queryParams,
			String keyFieldName);

	/**
	 * @param indexSearcherMono Lucene index searcher
	 * @param queryParams   the query parameters
	 * @param keyFieldName  the name of the key field
	 */
	@Override
	default Mono<Send<LuceneSearchResult>> collect(Mono<Send<LLIndexContext>> indexSearcherMono,
			LocalQueryParams queryParams,
			String keyFieldName) {
		return this.collect(indexSearcherMono.flux(), queryParams, keyFieldName);
	}
}
