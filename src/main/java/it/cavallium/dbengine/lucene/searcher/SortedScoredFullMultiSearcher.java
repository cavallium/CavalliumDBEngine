package it.cavallium.dbengine.lucene.searcher;

import io.net5.buffer.api.Send;
import it.cavallium.dbengine.database.LLKeyScore;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.disk.LLIndexSearchers;
import it.cavallium.dbengine.database.disk.LLTempLMDBEnv;
import it.cavallium.dbengine.lucene.FullDocs;
import it.cavallium.dbengine.lucene.LLFieldDoc;
import it.cavallium.dbengine.lucene.LuceneUtils;
import it.cavallium.dbengine.lucene.collector.LMDBFullFieldDocCollector;
import it.cavallium.dbengine.lucene.searcher.LLSearchTransformer.TransformerInput;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.IndexSearcher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class SortedScoredFullMultiSearcher implements MultiSearcher {

	protected static final Logger logger = LogManager.getLogger(SortedScoredFullMultiSearcher.class);

	private final LLTempLMDBEnv env;

	public SortedScoredFullMultiSearcher(LLTempLMDBEnv env) {
		this.env = env;
	}

	@Override
	public Mono<LuceneSearchResult> collectMulti(Mono<Send<LLIndexSearchers>> indexSearchersMono,
			LocalQueryParams queryParams,
			String keyFieldName,
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
						.search(indexSearchers.shards(), queryParams2)
						// Compute the results
						.transform(fullDocsMono -> this.computeResults(fullDocsMono, indexSearchers,
								keyFieldName, queryParams2))
						// Ensure that one LuceneSearchResult is always returned
						.single(),
				false));
	}

	/**
	 * Search effectively the raw results
	 */
	private Mono<FullDocs<LLFieldDoc>> search(Iterable<IndexSearcher> indexSearchers,
			LocalQueryParams queryParams) {
		return Mono
				.fromCallable(() -> {
					LLUtils.ensureBlocking();
					var totalHitsThreshold = queryParams.getTotalHitsThresholdLong();
					return LMDBFullFieldDocCollector.createSharedManager(env, queryParams.sort(), queryParams.limitInt(),
							totalHitsThreshold);
				})
				.flatMap(sharedManager -> Flux
						.fromIterable(indexSearchers)
						.flatMap(shard -> Mono.fromCallable(() -> {
							LLUtils.ensureBlocking();

							var collector = sharedManager.newCollector();
							assert queryParams.computePreciseHitsCount() == collector.scoreMode().isExhaustive();

							shard.search(queryParams.query(), collector);
							return collector;
						}))
						.collectList()
						.flatMap(collectors -> Mono.fromCallable(() -> {
							LLUtils.ensureBlocking();
							return sharedManager.reduce(collectors);
						}))
				);
	}

	/**
	 * Compute the results, extracting useful data
	 */
	private Mono<LuceneSearchResult> computeResults(Mono<FullDocs<LLFieldDoc>> dataMono,
			LLIndexSearchers indexSearchers,
			String keyFieldName,
			LocalQueryParams queryParams) {
		return dataMono.map(data -> {
			var totalHitsCount = LuceneUtils.convertTotalHitsCount(data.totalHits());

			Flux<LLKeyScore> hitsFlux = LuceneUtils
					.convertHits(data.iterate(queryParams.offsetLong()).map(LLFieldDoc::toFieldDoc),
							indexSearchers.shards(), keyFieldName, true)
					.take(queryParams.limitLong(), true);

			return new LuceneSearchResult(totalHitsCount, hitsFlux, indexSearchers::close);
		});
	}

	@Override
	public String getName() {
		return "sorted scored full multi";
	}
}
