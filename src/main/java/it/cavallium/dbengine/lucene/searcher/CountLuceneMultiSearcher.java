package it.cavallium.dbengine.lucene.searcher;

import it.cavallium.dbengine.client.query.QueryParser;
import it.cavallium.dbengine.client.query.current.data.QueryParams;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

public class CountLuceneMultiSearcher implements LuceneMultiSearcher {

	@Override
	public Mono<LuceneShardSearcher> createShardSearcher(QueryParams queryParams) {
		return Mono
				.fromCallable(() -> {
					AtomicLong totalHits = new AtomicLong(0);
					return new LuceneShardSearcher() {
						@Override
						public Mono<Void> searchOn(IndexSearcher indexSearcher, QueryParams queryParams, Scheduler scheduler) {
							return Mono
									.<Void>fromCallable(() -> {
										Query luceneQuery = QueryParser.toQuery(queryParams.query());
										//noinspection BlockingMethodInNonBlockingContext
										totalHits.addAndGet(indexSearcher.count(luceneQuery));
										return null;
									})
									.subscribeOn(scheduler);
						}

						@Override
						public Mono<LuceneSearchResult> collect(QueryParams queryParams, String keyFieldName, Scheduler scheduler) {
							return Mono.fromCallable(() -> new LuceneSearchResult(totalHits.get(), Flux.empty()));
						}
					};
				});
	}
}
