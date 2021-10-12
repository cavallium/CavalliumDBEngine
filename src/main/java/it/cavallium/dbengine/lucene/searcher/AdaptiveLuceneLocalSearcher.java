package it.cavallium.dbengine.lucene.searcher;

import io.net5.buffer.api.Send;
import io.net5.buffer.api.internal.ResourceSupport;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.disk.LLIndexSearcher;
import it.cavallium.dbengine.database.disk.LLIndexSearchers;
import it.cavallium.dbengine.database.disk.LLIndexSearchers.UnshardedIndexSearchers;
import it.cavallium.dbengine.lucene.searcher.LLSearchTransformer.TransformerInput;
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
		Mono<Send<LLIndexSearchers>> indexSearchersMono = indexSearcher
				.map(LLIndexSearchers::unsharded)
				.map(ResourceSupport::send);

		if (transformer == LLSearchTransformer.NO_TRANSFORMATION) {
			return transformedCollect(indexSearcher, queryParams, keyFieldName, transformer);
		} else {
			return LLUtils.usingSendResource(indexSearchersMono, indexSearchers -> transformer
							.transform(Mono.fromCallable(() -> new TransformerInput(indexSearchers, queryParams)))
							.flatMap(queryParams2 -> this
									.transformedCollect(indexSearcher, queryParams2, keyFieldName, LLSearchTransformer.NO_TRANSFORMATION)),
					true);
		}
	}

	@Override
	public String getName() {
		return "adaptivelocal";
	}

	public Mono<Send<LuceneSearchResult>> transformedCollect(Mono<Send<LLIndexSearcher>> indexSearcher,
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
