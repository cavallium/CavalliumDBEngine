package it.cavallium.dbengine.lucene.searcher;

import io.net5.buffer.api.Send;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.disk.LLIndexSearcher;
import org.apache.lucene.search.IndexSearcher;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class AdaptiveLuceneLocalSearcher implements LuceneLocalSearcher {

	private static final LuceneLocalSearcher localSearcher = new SimpleLuceneLocalSearcher();

	private static final LuceneLocalSearcher unscoredPagedLuceneLocalSearcher = new LocalLuceneWrapper(new UnscoredUnsortedContinuousLuceneMultiSearcher(), d -> {});

	private static final LuceneLocalSearcher countSearcher = new CountLuceneLocalSearcher();

	@Override
	public Mono<Send<LuceneSearchResult>> collect(Mono<Send<LLIndexSearcher>> indexSearcher,
			LocalQueryParams queryParams,
			String keyFieldName) {
		Mono<Send<LuceneSearchResult>> collectionMono;
		if (queryParams.limit() == 0) {
			collectionMono = countSearcher.collect(indexSearcher, queryParams, keyFieldName);
		} else if (!queryParams.isScored() && queryParams.offset() == 0 && queryParams.limit() >= 2147483630
				&& !queryParams.isSorted()) {
			collectionMono = unscoredPagedLuceneLocalSearcher.collect(indexSearcher, queryParams, keyFieldName);
		} else {
			collectionMono = localSearcher.collect(indexSearcher, queryParams, keyFieldName);
		}
		return Mono.fromRunnable(LLUtils::ensureBlocking).then(collectionMono);
	}
}
