package it.cavallium.dbengine.lucene.searcher;

import io.net5.buffer.api.Send;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.disk.LLIndexSearchers;
import it.cavallium.dbengine.lucene.collector.Buckets;
import it.cavallium.dbengine.lucene.collector.DecimalBucketMultiCollectorManager;
import java.util.List;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.warp.commonutils.log.Logger;
import org.warp.commonutils.log.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class DecimalBucketMultiSearcher {

	protected static final Logger logger = LoggerFactory.getLogger(DecimalBucketMultiSearcher.class);

	public Mono<Buckets> collectMulti(Mono<Send<LLIndexSearchers>> indexSearchersMono,
			BucketParams bucketParams,
			@NotNull List<Query> queries,
			@Nullable Query normalizationQuery) {

		return LLUtils.usingSendResource(indexSearchersMono, indexSearchers -> this
						// Search results
						.search(indexSearchers.shards(), bucketParams, queries, normalizationQuery)
						// Ensure that one result is always returned
						.single(),
				true);
	}

	private Mono<Buckets> search(Iterable<IndexSearcher> indexSearchers,
			BucketParams bucketParams,
			@NotNull List<Query> queries,
			@Nullable Query normalizationQuery) {
		return Mono
				.fromCallable(() -> {
					LLUtils.ensureBlocking();
					return new DecimalBucketMultiCollectorManager(bucketParams.min(),
							bucketParams.max(),
							bucketParams.buckets(),
							bucketParams.bucketFieldName(),
							bucketParams.valueSource(),
							queries,
							normalizationQuery
					);
				})
				.flatMap(cmm -> Flux
						.fromIterable(indexSearchers)
						.flatMap(shard -> Mono.fromCallable(() -> {
							LLUtils.ensureBlocking();
							return cmm.search(shard);
						}))
						.collectList()
						.flatMap(results -> Mono.fromCallable(() -> {
							LLUtils.ensureBlocking();
							return cmm.reduce(results);
						}))
				);
	}
}
