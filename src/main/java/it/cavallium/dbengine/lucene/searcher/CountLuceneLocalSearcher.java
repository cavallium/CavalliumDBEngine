package it.cavallium.dbengine.lucene.searcher;

import io.net5.buffer.api.Send;
import it.cavallium.dbengine.client.query.current.data.TotalHitsCount;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.disk.LLIndexContext;
import it.cavallium.dbengine.database.disk.LLIndexSearcher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class CountLuceneLocalSearcher implements LuceneLocalSearcher {

	@Override
	public Mono<Send<LuceneSearchResult>> collect(Mono<Send<LLIndexContext>> indexSearcherMono,
			LocalQueryParams queryParams,
			String keyFieldName) {
		return Mono
				.usingWhen(
						indexSearcherMono,
						indexSearcher -> Mono.fromCallable(() -> {
							try (var is = indexSearcher.receive()) {
								LLUtils.ensureBlocking();
								return is.getIndexSearcher().count(queryParams.query());
							}
						}).subscribeOn(Schedulers.boundedElastic()),
						is -> Mono.empty()
				)
				.map(count -> new LuceneSearchResult(TotalHitsCount.of(count, true), Flux.empty(), drop -> {}).send())
				.doOnDiscard(Send.class, Send::close);
	}
}
