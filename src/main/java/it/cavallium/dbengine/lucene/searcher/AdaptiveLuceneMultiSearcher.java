package it.cavallium.dbengine.lucene.searcher;

import io.net5.buffer.api.Send;
import it.cavallium.dbengine.database.disk.LLIndexSearcher;
import it.cavallium.dbengine.database.disk.LLIndexSearchers;
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
	public Mono<Send<LuceneSearchResult>> collectMulti(Mono<Send<LLIndexSearchers>> indexSearchersMono,
			LocalQueryParams queryParams,
			String keyFieldName,
			LLSearchTransformer transformer) {
		if (queryParams.limit() == 0) {
			return countLuceneMultiSearcher.collectMulti(indexSearchersMono, queryParams, keyFieldName, transformer);
		} else if (queryParams.isSorted() || queryParams.isScored()) {
			return scoredSimpleLuceneShardSearcher.collectMulti(indexSearchersMono, queryParams, keyFieldName, transformer);
		} else {
			return unscoredPagedLuceneMultiSearcher.collectMulti(indexSearchersMono, queryParams, keyFieldName, transformer);
		}
	}
}
