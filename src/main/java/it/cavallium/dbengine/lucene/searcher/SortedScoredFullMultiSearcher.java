package it.cavallium.dbengine.lucene.searcher;

import static it.cavallium.dbengine.client.UninterruptibleScheduler.uninterruptibleScheduler;
import static it.cavallium.dbengine.database.LLUtils.singleOrClose;

import io.netty5.buffer.api.Send;
import it.cavallium.dbengine.database.LLKeyScore;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.disk.LLIndexSearchers;
import it.cavallium.dbengine.database.disk.LLTempHugePqEnv;
import it.cavallium.dbengine.lucene.FullDocs;
import it.cavallium.dbengine.lucene.LLFieldDoc;
import it.cavallium.dbengine.lucene.LuceneUtils;
import it.cavallium.dbengine.lucene.hugepq.search.HugePqFullFieldDocCollector;
import java.io.IOException;
import java.io.UncheckedIOException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.IndexSearcher;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class SortedScoredFullMultiSearcher implements MultiSearcher {

	protected static final Logger LOG = LogManager.getLogger(SortedScoredFullMultiSearcher.class);

	private final LLTempHugePqEnv env;

	public SortedScoredFullMultiSearcher(LLTempHugePqEnv env) {
		this.env = env;
	}

	@Override
	public Mono<LuceneSearchResult> collectMulti(Mono<LLIndexSearchers> indexSearchersMono,
			LocalQueryParams queryParams,
			@Nullable String keyFieldName,
			GlobalQueryRewrite transformer) {
		if (transformer != GlobalQueryRewrite.NO_REWRITE) {
			return LuceneUtils.rewriteMulti(this, indexSearchersMono, queryParams, keyFieldName, transformer);
		}
		return singleOrClose(indexSearchersMono, indexSearchers -> this
				// Search results
				.search(indexSearchers.shards(), queryParams)
				// Compute the results
				.transform(fullDocsMono -> this.computeResults(fullDocsMono, indexSearchers, keyFieldName, queryParams))
				// Ensure that one LuceneSearchResult is always returned
				.single());
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
					return HugePqFullFieldDocCollector.createSharedManager(env, queryParams.sort(), queryParams.limitInt(),
							totalHitsThreshold);
				})
				.<FullDocs<LLFieldDoc>>flatMap(sharedManager -> Flux
						.fromIterable(indexSearchers)
						.flatMap(shard -> Mono.fromCallable(() -> {
							LLUtils.ensureBlocking();

							var collector = sharedManager.newCollector();
							try {
								assert queryParams.computePreciseHitsCount() == null
										|| queryParams.computePreciseHitsCount() == collector.scoreMode().isExhaustive();

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
								for (HugePqFullFieldDocCollector collector : collectors) {
									collector.close();
								}
								throw ex;
							}
						}))
				)
				.subscribeOn(uninterruptibleScheduler(Schedulers.boundedElastic()))
				.publishOn(Schedulers.parallel());
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

			return new LuceneSearchResult(totalHitsCount, hitsFlux, () -> {
				try {
					indexSearchers.close();
				} catch (UncheckedIOException e) {
					LOG.error("Can't close index searchers", e);
				}
				data.close();
			});
		});
	}

	@Override
	public String getName() {
		return "sorted scored full multi";
	}
}
