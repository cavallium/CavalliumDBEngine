package it.cavallium.dbengine.lucene.searcher;

import io.net5.buffer.api.Send;
import it.cavallium.dbengine.database.disk.LLIndexContext;
import it.cavallium.dbengine.database.disk.LLIndexSearcher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class AdaptiveLuceneMultiSearcher implements LuceneMultiSearcher {

	private static final LuceneMultiSearcher countLuceneMultiSearcher
			= new SimpleUnsortedUnscoredLuceneMultiSearcher(new CountLuceneLocalSearcher());

	private static final LuceneMultiSearcher scoredSimpleLuceneShardSearcher
			= new ScoredSimpleLuceneShardSearcher();

	private static final LuceneMultiSearcher unscoredPagedLuceneMultiSearcher
			= new SimpleUnsortedUnscoredLuceneMultiSearcher(new SimpleLuceneLocalSearcher());

	@Override
	public Mono<Send<LuceneSearchResult>> collect(Flux<Send<LLIndexContext>> indexSearchersFlux,
			LocalQueryParams queryParams,
			String keyFieldName) {
		if (queryParams.limit() == 0) {
			return countLuceneMultiSearcher.collect(indexSearchersFlux, queryParams, keyFieldName);
		} else if (queryParams.isSorted() || queryParams.isScored()) {
			return scoredSimpleLuceneShardSearcher.collect(indexSearchersFlux, queryParams, keyFieldName);
		} else {
			return unscoredPagedLuceneMultiSearcher.collect(indexSearchersFlux, queryParams, keyFieldName);
		}
	}
}
