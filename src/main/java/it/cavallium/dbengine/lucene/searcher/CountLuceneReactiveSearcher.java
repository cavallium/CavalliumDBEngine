package it.cavallium.dbengine.lucene.searcher;

import it.cavallium.dbengine.database.LLKeyScore;
import it.cavallium.dbengine.lucene.LuceneUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

public class CountLuceneReactiveSearcher implements LuceneReactiveSearcher {


	@SuppressWarnings("BlockingMethodInNonBlockingContext")
	@Override
	public Mono<LuceneReactiveSearchInstance> search(IndexSearcher indexSearcher,
			Query query,
			int offset,
			int limit,
			@Nullable Sort luceneSort,
			ScoreMode scoreMode,
			@Nullable Float minCompetitiveScore,
			String keyFieldName,
			Scheduler scheduler) {
		return Mono
				.fromCallable(() -> new LuceneReactiveSearchInstance(indexSearcher.count(query), Flux.empty()))
				.subscribeOn(scheduler);
	}
}
