package it.cavallium.dbengine.lucene.searcher;

import io.net5.buffer.api.Send;
import io.net5.buffer.api.internal.ResourceSupport;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.disk.LLIndexSearcher;
import it.cavallium.dbengine.database.disk.LLIndexSearchers;
import it.cavallium.dbengine.lucene.searcher.LLSearchTransformer.TransformerInput;
import reactor.core.publisher.Mono;

public class AdaptiveLocalSearcher implements LocalSearcher {

	private static final LocalSearcher localSearcher = new PagedLocalSearcher();

	private static final LocalSearcher countSearcher = new CountMultiSearcher();

	@Override
	public Mono<LuceneSearchResult> collect(Mono<Send<LLIndexSearcher>> indexSearcher,
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

	public Mono<LuceneSearchResult> transformedCollect(Mono<Send<LLIndexSearcher>> indexSearcher,
			LocalQueryParams queryParams,
			String keyFieldName,
			LLSearchTransformer transformer) {
		if (queryParams.limitLong() == 0) {
			return countSearcher.collect(indexSearcher, queryParams, keyFieldName, transformer);
		} else {
			return localSearcher.collect(indexSearcher, queryParams, keyFieldName, transformer);
		}
	}
}
