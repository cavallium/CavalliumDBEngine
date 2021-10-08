package it.cavallium.dbengine.lucene.searcher;

import io.net5.buffer.api.Send;
import it.cavallium.dbengine.database.disk.LLIndexSearcher;
import it.cavallium.dbengine.database.disk.LLIndexSearchers;
import org.apache.lucene.search.IndexSearcher;
import reactor.core.publisher.Mono;

public class AdaptiveLuceneLocalSearcher implements LuceneLocalSearcher {

	private static final LuceneLocalSearcher localSearcher = new SimpleLuceneLocalSearcher();

	private static final LuceneLocalSearcher countSearcher = new CountLuceneLocalSearcher();

	@Override
	public Mono<Send<LuceneSearchResult>> collect(Mono<Send<LLIndexSearcher>> indexSearcher,
			LocalQueryParams queryParams,
			String keyFieldName,
			LLSearchTransformer transformer) {
		if (queryParams.limit() == 0) {
			return countSearcher.collect(indexSearcher, queryParams, keyFieldName, transformer);
		} else {
			return localSearcher.collect(indexSearcher, queryParams, keyFieldName, transformer);
		}
	}
}
