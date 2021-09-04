package it.cavallium.dbengine.lucene.searcher;

import org.apache.lucene.search.IndexSearcher;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

public class LocalLuceneWrapper implements LuceneLocalSearcher {

	private final LuceneMultiSearcher luceneMultiSearcher;

	public LocalLuceneWrapper(LuceneMultiSearcher luceneMultiSearcher) {
		this.luceneMultiSearcher = luceneMultiSearcher;
	}

	@Override
	public Mono<LuceneSearchResult> collect(IndexSearcher indexSearcher,
			Mono<Void> releaseIndexSearcher,
			LocalQueryParams queryParams,
			String keyFieldName) {
		var shardSearcher = luceneMultiSearcher.createShardSearcher(queryParams);
		return shardSearcher
				.flatMap(luceneShardSearcher -> luceneShardSearcher
						.searchOn(indexSearcher, releaseIndexSearcher, queryParams)
						.then(luceneShardSearcher.collect(queryParams, keyFieldName))
				);
	}
}
