package it.cavallium.dbengine.lucene.searcher;

import io.net5.buffer.api.Send;
import it.cavallium.dbengine.client.query.current.data.TotalHitsCount;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.disk.LLIndexSearcher;
import it.cavallium.dbengine.database.disk.LLIndexSearchers;
import it.cavallium.dbengine.lucene.searcher.LLSearchTransformer.TransformerInput;
import org.apache.lucene.search.IndexSearcher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class CountLuceneLocalSearcher implements LuceneLocalSearcher {

	@Override
	public Mono<Send<LuceneSearchResult>> collect(Mono<Send<LLIndexSearcher>> indexSearcherMono,
			LocalQueryParams queryParams,
			String keyFieldName,
			LLSearchTransformer transformer) {
		return Mono
				.usingWhen(
						indexSearcherMono,
						indexSearcher -> {
							var queryParamsMono = transformer
									.transform(Mono.fromSupplier(() -> new TransformerInput(LLIndexSearchers.unsharded(indexSearcher),
											queryParams)));

							return queryParamsMono.flatMap(queryParams2 -> Mono.fromCallable(() -> {
								try (var is = indexSearcher.receive()) {
									LLUtils.ensureBlocking();
									return is.getIndexSearcher().count(queryParams2.query());
								}
							}).subscribeOn(Schedulers.boundedElastic()));
						},
						is -> Mono.empty()
				)
				.map(count -> new LuceneSearchResult(TotalHitsCount.of(count, true), Flux.empty(), null).send())
				.doOnDiscard(Send.class, Send::close);
	}
}
