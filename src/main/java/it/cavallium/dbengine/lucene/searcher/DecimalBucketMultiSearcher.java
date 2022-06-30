package it.cavallium.dbengine.lucene.searcher;

import static it.cavallium.dbengine.client.UninterruptibleScheduler.uninterruptibleScheduler;

import io.netty5.buffer.api.Send;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.disk.LLIndexSearchers;
import it.cavallium.dbengine.lucene.collector.Buckets;
import it.cavallium.dbengine.lucene.collector.DecimalBucketMultiCollectorManager;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class DecimalBucketMultiSearcher {

	protected static final Logger logger = LogManager.getLogger(DecimalBucketMultiSearcher.class);

	public Mono<Buckets> collectMulti(Mono<LLIndexSearchers> indexSearchersMono,
			BucketParams bucketParams,
			@NotNull List<Query> queries,
			@Nullable Query normalizationQuery) {

		return Mono.usingWhen(indexSearchersMono, indexSearchers -> this
				// Search results
				.search(indexSearchers.shards(), bucketParams, queries, normalizationQuery)
				// Ensure that one result is always returned
				.single(), indexSearchers -> Mono.fromCallable(() -> {
			//noinspection BlockingMethodInNonBlockingContext
			indexSearchers.close();
			return null;
		}).subscribeOn(uninterruptibleScheduler(Schedulers.boundedElastic()))).publishOn(Schedulers.parallel());
	}

	private Mono<Buckets> search(Iterable<IndexSearcher> indexSearchers,
			BucketParams bucketParams,
			@NotNull List<Query> queries,
			@Nullable Query normalizationQuery) {
		return Mono.defer(() -> {
			var cmm = new DecimalBucketMultiCollectorManager(bucketParams.min(),
					bucketParams.max(),
					bucketParams.buckets(),
					bucketParams.bucketFieldName(),
					bucketParams.valueSource(),
					queries,
					normalizationQuery,
					bucketParams.collectionRate(),
					bucketParams.sampleSize()
			);
			return Flux
					.fromIterable(indexSearchers)
					.flatMap(shard -> Mono.fromCallable(() -> {
						LLUtils.ensureBlocking();
						return cmm.search(shard);
					}))
					.collectList()
					.flatMap(results -> Mono.fromSupplier(() -> cmm.reduce(results)))
					.subscribeOn(uninterruptibleScheduler(Schedulers.boundedElastic()))
					.publishOn(Schedulers.parallel());
		});
	}
}
