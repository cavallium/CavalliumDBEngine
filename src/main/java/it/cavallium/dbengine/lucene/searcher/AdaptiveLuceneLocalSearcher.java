package it.cavallium.dbengine.lucene.searcher;

import io.net5.buffer.api.Send;
import it.cavallium.dbengine.database.disk.LLIndexContext;
import it.cavallium.dbengine.database.disk.LLIndexSearcher;
import reactor.core.publisher.Mono;

public class AdaptiveLuceneLocalSearcher implements LuceneLocalSearcher {

	private static final LuceneLocalSearcher localSearcher = new SimpleLuceneLocalSearcher();

	private static final LuceneLocalSearcher countSearcher = new CountLuceneLocalSearcher();

	@Override
	public Mono<Send<LuceneSearchResult>> collect(Mono<Send<LLIndexContext>> indexSearcher,
			LocalQueryParams queryParams,
			String keyFieldName) {
		if (queryParams.limit() == 0) {
			return countSearcher.collect(indexSearcher, queryParams, keyFieldName);
		} else {
			return localSearcher.collect(indexSearcher, queryParams, keyFieldName);
		}
	}
}
