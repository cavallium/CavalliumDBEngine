package it.cavallium.dbengine.lucene.searcher;

import it.cavallium.dbengine.client.query.QueryParser;
import it.cavallium.dbengine.client.query.current.data.QueryParams;
import it.cavallium.dbengine.database.LLKeyScore;
import it.cavallium.dbengine.lucene.LuceneUtils;
import java.util.List;
import java.util.Objects;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

public class SimpleLuceneLocalSearcher implements LuceneLocalSearcher {

	@Override
	public Mono<LuceneSearchResult> collect(IndexSearcher indexSearcher,
			QueryParams queryParams,
			String keyFieldName,
			Scheduler scheduler) {
		return Mono
				.fromCallable(() -> {
					Objects.requireNonNull(queryParams.scoreMode(), "ScoreMode must not be null");
					Query luceneQuery = QueryParser.toQuery(queryParams.query());
					Sort luceneSort = QueryParser.toSort(queryParams.sort());
					ScoreMode luceneScoreMode = QueryParser.toScoreMode(queryParams.scoreMode());
					TopDocs topDocs = TopDocsSearcher.getTopDocs(indexSearcher,
							luceneQuery,
							luceneSort,
							LuceneUtils.safeLongToInt(queryParams.offset() + queryParams.limit()),
							null,
							luceneScoreMode.needsScores(),
							1000,
							LuceneUtils.safeLongToInt(queryParams.offset()), LuceneUtils.safeLongToInt(queryParams.limit()));
					Flux<LLKeyScore> hitsMono = LuceneMultiSearcher
							.convertHits(
									topDocs.scoreDocs,
									IndexSearchers.unsharded(indexSearcher),
									keyFieldName,
									scheduler
							)
							.take(queryParams.limit(), true);
					return new LuceneSearchResult(topDocs.totalHits.value, hitsMono);
				})
				.subscribeOn(scheduler);
	}
}
