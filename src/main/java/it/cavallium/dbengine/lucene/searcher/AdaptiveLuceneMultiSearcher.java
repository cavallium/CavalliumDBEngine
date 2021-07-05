package it.cavallium.dbengine.lucene.searcher;

import it.cavallium.dbengine.client.query.QueryParser;
import it.cavallium.dbengine.client.query.current.data.QueryParams;
import org.apache.lucene.search.Sort;
import reactor.core.publisher.Mono;

public class AdaptiveLuceneMultiSearcher implements LuceneMultiSearcher {

	private static final LuceneMultiSearcher sharedSortedLuceneMultiSearcher = new SharedSortedLuceneMultiSearcher();

	private static final LuceneMultiSearcher unscoredLuceneMultiSearcher = new UnscoredLuceneMultiSearcher();

	private static final LuceneMultiSearcher countLuceneMultiSearcher = new CountLuceneMultiSearcher();

	@Override
	public Mono<LuceneShardSearcher> createShardSearcher(QueryParams queryParams) {
		Sort luceneSort = QueryParser.toSort(queryParams.sort());
		if (queryParams.limit() <= 0) {
			return countLuceneMultiSearcher.createShardSearcher(queryParams);
		} else if ((luceneSort != null && luceneSort != Sort.RELEVANCE) || queryParams.scoreMode().computeScores()) {
			return sharedSortedLuceneMultiSearcher.createShardSearcher(queryParams);
		} else {
			return unscoredLuceneMultiSearcher.createShardSearcher(queryParams);
		}
	}
}
