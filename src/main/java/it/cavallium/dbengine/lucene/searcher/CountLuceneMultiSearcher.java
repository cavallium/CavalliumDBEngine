package it.cavallium.dbengine.lucene.searcher;

import it.cavallium.dbengine.client.query.QueryParser;
import it.cavallium.dbengine.client.query.current.data.QueryParams;
import it.cavallium.dbengine.client.query.current.data.TotalHitsCount;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

public class CountLuceneMultiSearcher implements LuceneMultiSearcher {

	@Override
	public Mono<LuceneShardSearcher> createShardSearcher(LocalQueryParams queryParams) {
		return Mono
				.fromCallable(() -> {
					AtomicLong totalHits = new AtomicLong(0);
					ConcurrentLinkedQueue<Mono<Void>> release = new ConcurrentLinkedQueue<>();
					return new LuceneShardSearcher() {
						@Override
						public Mono<Void> searchOn(IndexSearcher indexSearcher,
								Mono<Void> releaseIndexSearcher,
								LocalQueryParams queryParams,
								Scheduler scheduler) {
							return Mono
									.<Void>fromCallable(() -> {
										//noinspection BlockingMethodInNonBlockingContext
										totalHits.addAndGet(indexSearcher.count(queryParams.query()));
										release.add(releaseIndexSearcher);
										return null;
									})
									.subscribeOn(scheduler);
						}

						@Override
						public Mono<LuceneSearchResult> collect(LocalQueryParams queryParams, String keyFieldName, Scheduler scheduler) {
							return Mono.fromCallable(() -> new LuceneSearchResult(TotalHitsCount.of(totalHits.get(), true), Flux.empty(), Mono.when(release)));
						}
					};
				});
	}
}
