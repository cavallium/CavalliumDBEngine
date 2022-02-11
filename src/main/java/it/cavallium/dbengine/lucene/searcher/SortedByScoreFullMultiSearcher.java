package it.cavallium.dbengine.lucene.searcher;

import static it.cavallium.dbengine.client.UninterruptibleScheduler.uninterruptibleScheduler;

import io.net5.buffer.api.Send;
import it.cavallium.dbengine.database.LLKeyScore;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.disk.LLIndexSearchers;
import it.cavallium.dbengine.database.disk.LLTempLMDBEnv;
import it.cavallium.dbengine.lucene.LuceneUtils;
import it.cavallium.dbengine.lucene.FullDocs;
import it.cavallium.dbengine.lucene.LLScoreDoc;
import org.apache.lucene.search.LMDBFullScoreDocCollector;
import java.io.IOException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.IndexSearcher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class SortedByScoreFullMultiSearcher implements MultiSearcher {

	protected static final Logger logger = LogManager.getLogger(SortedByScoreFullMultiSearcher.class);

	private final LLTempLMDBEnv env;

	public SortedByScoreFullMultiSearcher(LLTempLMDBEnv env) {
		this.env = env;
	}

	@Override
	public Mono<LuceneSearchResult> collectMulti(Mono<Send<LLIndexSearchers>> indexSearchersMono,
			LocalQueryParams queryParams,
			String keyFieldName,
			GlobalQueryRewrite transformer) {
		Mono<LocalQueryParams> queryParamsMono;
		if (transformer == GlobalQueryRewrite.NO_REWRITE) {
			queryParamsMono = Mono.just(queryParams);
		} else {
			queryParamsMono = indexSearchersMono
					.publishOn(uninterruptibleScheduler(Schedulers.boundedElastic()))
					.handle((indexSearchers, sink) -> {
						try {
							sink.next(transformer.rewrite(indexSearchers.receive(), queryParams));
						} catch (IOException ex) {
							sink.error(ex);
						}
					});
		}

		return queryParamsMono.flatMap(queryParams2 -> {
			if (queryParams2.isSorted() && !queryParams2.isSortedByScore()) {
				throw new IllegalArgumentException(SortedByScoreFullMultiSearcher.this.getClass().getSimpleName()
						+ " doesn't support sorted queries");
			}

			return LLUtils.usingSendResource(indexSearchersMono, indexSearchers -> this
							// Search results
							.search(indexSearchers.shards(), queryParams2)
							// Compute the results
							.transform(fullDocsMono -> this.computeResults(fullDocsMono, indexSearchers,
									keyFieldName, queryParams2))
							// Ensure that one LuceneSearchResult is always returned
							.single(),
					false);
		});
	}

	/**
	 * Search effectively the raw results
	 */
	private Mono<FullDocs<LLScoreDoc>> search(Iterable<IndexSearcher> indexSearchers,
			LocalQueryParams queryParams) {
		return Mono
				.fromCallable(() -> {
					LLUtils.ensureBlocking();
					var totalHitsThreshold = queryParams.getTotalHitsThresholdLong();
					return LMDBFullScoreDocCollector.createSharedManager(env, queryParams.limitLong(), totalHitsThreshold);
				})
				.flatMap(sharedManager -> Flux
						.fromIterable(indexSearchers)
						.flatMap(shard -> Mono.fromCallable(() -> {
							LLUtils.ensureBlocking();

							var collector = sharedManager.newCollector();
							try {
								assert queryParams.computePreciseHitsCount() == null ||
										queryParams.computePreciseHitsCount() == collector.scoreMode().isExhaustive();

								shard.search(queryParams.query(), collector);
								return collector;
							} catch (Throwable ex) {
								collector.close();
								throw ex;
							}
						}))
						.collectList()
						.flatMap(collectors -> Mono.fromCallable(() -> {
							try {
								LLUtils.ensureBlocking();
								return sharedManager.reduce(collectors);
							} catch (Throwable ex) {
								for (LMDBFullScoreDocCollector collector : collectors) {
									collector.close();
								}
								throw ex;
							}
						}))
				);
	}

	/**
	 * Compute the results, extracting useful data
	 */
	private Mono<LuceneSearchResult> computeResults(Mono<FullDocs<LLScoreDoc>> dataMono,
			LLIndexSearchers indexSearchers,
			String keyFieldName,
			LocalQueryParams queryParams) {
		return dataMono.map(data -> {
			var totalHitsCount = LuceneUtils.convertTotalHitsCount(data.totalHits());

			Flux<LLKeyScore> hitsFlux = LuceneUtils
					.convertHits(data.iterate(queryParams.offsetLong()).map(LLScoreDoc::toScoreDoc),
							indexSearchers.shards(), keyFieldName, true)
					.take(queryParams.limitLong(), true);

			return new LuceneSearchResult(totalHitsCount, hitsFlux, () -> {
				indexSearchers.close();
				try {
					data.close();
				} catch (Exception e) {
					logger.error("Failed to discard data", e);
				}
			});
		});
	}

	@Override
	public String getName() {
		return "sorted by score full multi";
	}
}
