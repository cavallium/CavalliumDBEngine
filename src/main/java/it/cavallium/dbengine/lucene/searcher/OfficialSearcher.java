package it.cavallium.dbengine.lucene.searcher;

import static it.cavallium.dbengine.client.UninterruptibleScheduler.uninterruptibleScheduler;

import io.net5.buffer.api.Send;
import it.cavallium.dbengine.database.LLKeyScore;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.disk.LLIndexSearchers;
import it.cavallium.dbengine.lucene.LuceneUtils;
import java.io.IOException;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopScoreDocCollector;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class OfficialSearcher implements MultiSearcher {

	protected static final Logger logger = LogManager.getLogger(OfficialSearcher.class);

	public OfficialSearcher() {
	}

	@Override
	public Mono<LuceneSearchResult> collectMulti(Mono<Send<LLIndexSearchers>> indexSearchersMono,
			LocalQueryParams queryParams,
			@Nullable String keyFieldName,
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
	@SuppressWarnings({"unchecked", "rawtypes"})
	private Mono<TopDocs> search(Iterable<IndexSearcher> indexSearchers,
			LocalQueryParams queryParams) {
		return Mono
				.fromCallable(() -> {
					LLUtils.ensureBlocking();
					var totalHitsThreshold = queryParams.getTotalHitsThresholdInt();
					if (queryParams.isSorted() && !queryParams.isSortedByScore()) {
						return TopFieldCollector.createSharedManager(queryParams.sort(), queryParams.limitInt(), null,
								totalHitsThreshold);
					} else {
						return TopScoreDocCollector.createSharedManager(queryParams.limitInt(), null, totalHitsThreshold);
					}
				})
				.flatMap(sharedManager -> Flux
						.fromIterable(indexSearchers)
						.flatMap(shard -> Mono.fromCallable(() -> {
							LLUtils.ensureBlocking();

							var collector = sharedManager.newCollector();
							assert queryParams.computePreciseHitsCount() == null
									|| (queryParams.computePreciseHitsCount() == collector.scoreMode().isExhaustive());

							shard.search(queryParams.query(), LuceneUtils.withTimeout(collector, queryParams.timeout()));
							return collector;
						}))
						.collectList()
						.flatMap(collectors -> Mono.fromCallable(() -> {
							LLUtils.ensureBlocking();
							return sharedManager.reduce((List) collectors);
						}))
				);
	}

	/**
	 * Compute the results, extracting useful data
	 */
	private Mono<LuceneSearchResult> computeResults(Mono<TopDocs> dataMono,
			LLIndexSearchers indexSearchers,
			String keyFieldName,
			LocalQueryParams queryParams) {
		return dataMono.map(data -> {
			var totalHitsCount = LuceneUtils.convertTotalHitsCount(data.totalHits);

			Flux<LLKeyScore> hitsFlux = LuceneUtils
					.convertHits(Flux.fromArray(data.scoreDocs),
							indexSearchers.shards(), keyFieldName, true)
					.skip(queryParams.offsetLong())
					.take(queryParams.limitLong(), true);

			return new LuceneSearchResult(totalHitsCount, hitsFlux, indexSearchers::close);
		});
	}

	@Override
	public String getName() {
		return "official";
	}
}
