package it.cavallium.dbengine.lucene.searcher;

import io.net5.buffer.api.Resource;
import io.net5.buffer.api.Send;
import it.cavallium.dbengine.database.disk.LLIndexSearcher;
import org.jetbrains.annotations.NotNull;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

public interface LuceneMultiSearcher extends LuceneLocalSearcher {

	/**
	 * @param queryParams the query parameters
	 * @param keyFieldName the name of the key field
	 */
	Mono<Send<LuceneSearchResult>> collect(Flux<Send<LLIndexSearcher>> indexSearchersFlux,
			LocalQueryParams queryParams,
			String keyFieldName);

	/**
	 * @param indexSearcherMono Lucene index searcher
	 * @param queryParams   the query parameters
	 * @param keyFieldName  the name of the key field
	 */
	@Override
	default Mono<Send<LuceneSearchResult>> collect(Mono<Send<LLIndexSearcher>> indexSearcherMono,
			LocalQueryParams queryParams,
			String keyFieldName) {
		return this.collect(indexSearcherMono.flux(), queryParams, keyFieldName);
	}
}
