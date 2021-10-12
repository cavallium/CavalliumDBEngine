package it.cavallium.dbengine.lucene.searcher;

import io.net5.buffer.api.Send;
import it.cavallium.dbengine.database.LLKeyScore;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.disk.LLIndexSearchers;
import it.cavallium.dbengine.database.disk.LLTempLMDBEnv;
import it.cavallium.dbengine.lucene.LuceneUtils;
import it.cavallium.dbengine.lucene.FullDocs;
import it.cavallium.dbengine.lucene.LLScoreDoc;
import it.cavallium.dbengine.lucene.collector.LMDBFullScoreDocCollector;
import it.cavallium.dbengine.lucene.searcher.LLSearchTransformer.TransformerInput;
import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Sort;
import org.warp.commonutils.log.Logger;
import org.warp.commonutils.log.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class UnsortedScoredFullLuceneMultiSearcher implements LuceneMultiSearcher, Closeable {

	protected static final Logger logger = LoggerFactory.getLogger(UnsortedScoredFullLuceneMultiSearcher.class);

	private final LLTempLMDBEnv env;

	public UnsortedScoredFullLuceneMultiSearcher() throws IOException {
		this.env = new LLTempLMDBEnv();
	}

	@Override
	public Mono<Send<LuceneSearchResult>> collectMulti(Mono<Send<LLIndexSearchers>> indexSearchersMono,
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

		return queryParamsMono.flatMap(queryParams2 -> {
			Objects.requireNonNull(queryParams2.scoreMode(), "ScoreMode must not be null");
			if (queryParams2.sort() != null && queryParams2.sort() != Sort.RELEVANCE) {
				throw new IllegalArgumentException(UnsortedScoredFullLuceneMultiSearcher.this.getClass().getSimpleName()
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
					var totalHitsThreshold = LuceneUtils.totalHitsThreshold();
					return LMDBFullScoreDocCollector.createSharedManager(env, totalHitsThreshold);
				})
				.flatMap(sharedManager -> Flux
						.fromIterable(indexSearchers)
						.flatMap(shard -> Mono.fromCallable(() -> {
							LLUtils.ensureBlocking();
							var collector = sharedManager.newCollector();
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
	private Mono<Send<LuceneSearchResult>> computeResults(Mono<FullDocs<LLScoreDoc>> dataMono,
			LLIndexSearchers indexSearchers,
			String keyFieldName,
			LocalQueryParams queryParams) {
		return dataMono.map(data -> {
			var totalHitsCount = LuceneUtils.convertTotalHitsCount(data.totalHits());

			Flux<LLKeyScore> hitsFlux = LuceneUtils
					.convertHits(data.iterate(queryParams.offset()).map(LLScoreDoc::toScoreDoc),
							indexSearchers.shards(), keyFieldName, true)
					.take(queryParams.limit(), true);

			return new LuceneSearchResult(totalHitsCount, hitsFlux, indexSearchers::close).send();
		});
	}

	@Override
	public void close() throws IOException {
		env.close();
	}

	@Override
	public String getName() {
		return "scoredfullmulti";
	}
}
