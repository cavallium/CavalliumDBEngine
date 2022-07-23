package it.cavallium.dbengine.lucene.searcher;

import static it.cavallium.dbengine.client.UninterruptibleScheduler.uninterruptibleScheduler;
import static it.cavallium.dbengine.database.LLUtils.singleOrClose;
import static it.cavallium.dbengine.lucene.LuceneUtils.luceneScheduler;
import static it.cavallium.dbengine.lucene.LuceneUtils.sum;
import static java.util.Objects.requireNonNull;

import it.cavallium.dbengine.client.query.current.data.TotalHitsCount;
import it.cavallium.dbengine.database.LLKeyScore;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.disk.LLIndexSearchers;
import it.cavallium.dbengine.lucene.LuceneCloseable;
import it.cavallium.dbengine.lucene.LuceneUtils;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TopScoreDocCollector;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class StandardSearcher implements MultiSearcher {

	protected static final Logger LOG = LogManager.getLogger(StandardSearcher.class);

	public StandardSearcher() {
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
	@SuppressWarnings({"unchecked", "rawtypes"})
	private Mono<TopDocs> search(Iterable<IndexSearcher> indexSearchers,
			LocalQueryParams queryParams) {
		return Mono
				.fromCallable(() -> {
					LLUtils.ensureBlocking();
					var totalHitsThreshold = queryParams.getTotalHitsThresholdInt();
					if (queryParams.isSorted() && !queryParams.isSortedByScore()) {
						return TopFieldCollector.createSharedManager(queryParams.sort(),
								queryParams.limitInt(), null, totalHitsThreshold);
					} else {
						return TopScoreDocCollector.createSharedManager(queryParams.limitInt(), null, totalHitsThreshold);
					}
				})
				.transform(LuceneUtils::scheduleLucene)
				.flatMap(sharedManager -> Flux.fromIterable(indexSearchers).flatMapSequential(shard -> Mono.fromCallable(() -> {
					LLUtils.ensureBlocking();
					var collector = sharedManager.newCollector();
					assert queryParams.computePreciseHitsCount() == null || (queryParams.computePreciseHitsCount() == collector
							.scoreMode().isExhaustive());

					shard.search(queryParams.query(), LuceneUtils.withTimeout(collector, queryParams.timeout()));
					return collector;
				}).subscribeOn(luceneScheduler())).collectList().flatMap(collectors -> Mono.fromCallable(() -> {
					LLUtils.ensureBlocking();
					if (collectors.size() <= 1) {
						return sharedManager.reduce((List) collectors);
					} else if (queryParams.isSorted() && !queryParams.isSortedByScore()) {
						final TopFieldDocs[] topDocs = new TopFieldDocs[collectors.size()];
						int i = 0;
						for (var collector : collectors) {
							var topFieldDocs = ((TopFieldCollector) collector).topDocs();
							for (ScoreDoc scoreDoc : topFieldDocs.scoreDocs) {
								scoreDoc.shardIndex = i;
							}
							topDocs[i++] = topFieldDocs;
						}
						return TopDocs.merge(requireNonNull(queryParams.sort()), 0, queryParams.limitInt(), topDocs);
					} else {
						final TopDocs[] topDocs = new TopDocs[collectors.size()];
						int i = 0;
						for (var collector : collectors) {
							var topScoreDocs = collector.topDocs();
							for (ScoreDoc scoreDoc : topScoreDocs.scoreDocs) {
								scoreDoc.shardIndex = i;
							}
							topDocs[i++] = topScoreDocs;
						}
						return TopDocs.merge(0, queryParams.limitInt(), topDocs);
					}
				}).subscribeOn(luceneScheduler())))
				.publishOn(Schedulers.parallel());
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

			return new MyLuceneSearchResult(totalHitsCount, hitsFlux, indexSearchers);
		});
	}

	@Override
	public String getName() {
		return "standard";
	}

	private static class MyLuceneSearchResult extends LuceneSearchResult implements LuceneCloseable {

		private final LLIndexSearchers indexSearchers;

		public MyLuceneSearchResult(TotalHitsCount totalHitsCount,
				Flux<LLKeyScore> hitsFlux,
				LLIndexSearchers indexSearchers) {
			super(totalHitsCount, hitsFlux);
			this.indexSearchers = indexSearchers;
		}

		@Override
		protected void onClose() {
			try {
				indexSearchers.close();
			} catch (Throwable e) {
				LOG.error("Can't close index searchers", e);
			}
			super.onClose();
		}
	}
}
