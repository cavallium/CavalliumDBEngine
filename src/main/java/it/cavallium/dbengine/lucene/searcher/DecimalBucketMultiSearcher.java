package it.cavallium.dbengine.lucene.searcher;

import io.net5.buffer.api.Send;
import it.cavallium.dbengine.database.LLKeyScore;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.disk.LLIndexSearchers;
import it.cavallium.dbengine.database.disk.LLTempLMDBEnv;
import it.cavallium.dbengine.lucene.FullDocs;
import it.cavallium.dbengine.lucene.LLFieldDoc;
import it.cavallium.dbengine.lucene.LuceneUtils;
import it.cavallium.dbengine.lucene.collector.DecimalBucketCollectorMultiManager;
import it.cavallium.dbengine.lucene.collector.LMDBFullFieldDocCollector;
import it.cavallium.dbengine.lucene.searcher.LLSearchTransformer.TransformerInput;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import org.apache.lucene.search.IndexSearcher;
import org.warp.commonutils.log.Logger;
import org.warp.commonutils.log.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class DecimalBucketMultiSearcher {

	protected static final Logger logger = LoggerFactory.getLogger(DecimalBucketMultiSearcher.class);

	public Mono<DoubleArrayList> collectMulti(Mono<Send<LLIndexSearchers>> indexSearchersMono,
			BucketParams bucketParams,
			LocalQueryParams queryParams,
			LLSearchTransformer transformer) {
		Mono<LocalQueryParams> queryParamsMono;
		if (transformer == LLSearchTransformer.NO_TRANSFORMATION) {
			queryParamsMono = Mono.just(queryParams);
		} else {
			queryParamsMono = LLUtils.usingSendResource(indexSearchersMono, indexSearchers -> transformer.transform(Mono
					.fromSupplier(() -> new TransformerInput(indexSearchers, queryParams))), true);
		}

		return queryParamsMono.flatMap(queryParams2 -> LLUtils.usingSendResource(indexSearchersMono, indexSearchers -> this
						// Search results
						.search(indexSearchers.shards(), bucketParams, queryParams2)
						// Ensure that one result is always returned
						.single(),
				true));
	}

	private Mono<DoubleArrayList> search(Iterable<IndexSearcher> indexSearchers,
			BucketParams bucketParams,
			LocalQueryParams queryParams) {
		return Mono
				.fromCallable(() -> {
					LLUtils.ensureBlocking();
					return new DecimalBucketCollectorMultiManager(bucketParams.min(),
							bucketParams.max(),
							bucketParams.buckets(),
							bucketParams.bucketFieldName(),
							bucketParams.valueFieldName()
					);
				})
				.flatMap(cmm -> Flux
						.fromIterable(indexSearchers)
						.flatMap(shard -> Mono.fromCallable(() -> {
							LLUtils.ensureBlocking();

							var collector = cmm.get(shard);

							return shard.search(queryParams.query(), collector);
						}))
						.collectList()
						.flatMap(results -> Mono.fromCallable(() -> {
							LLUtils.ensureBlocking();
							return cmm.reduce(results);
						}))
				);
	}
}
