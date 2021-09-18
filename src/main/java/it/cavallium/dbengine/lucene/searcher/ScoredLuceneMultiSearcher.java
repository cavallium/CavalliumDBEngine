package it.cavallium.dbengine.lucene.searcher;

import static it.cavallium.dbengine.lucene.searcher.PaginationInfo.FIRST_PAGE_LIMIT;
import static it.cavallium.dbengine.lucene.searcher.PaginationInfo.MAX_SINGLE_SEARCH_LIMIT;

import it.cavallium.dbengine.lucene.LuceneUtils;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldCollector;
import reactor.core.publisher.Mono;

public class ScoredLuceneMultiSearcher implements LuceneMultiSearcher {

	@Override
	public Mono<LuceneMultiSearcher> createShardSearcher(LocalQueryParams queryParams) {
		return Mono
				.fromCallable(() -> {
					Sort luceneSort = queryParams.sort();
					if (luceneSort == null) {
						luceneSort = Sort.RELEVANCE;
					}
					PaginationInfo paginationInfo;
					if (queryParams.limit() <= MAX_SINGLE_SEARCH_LIMIT) {
						paginationInfo = new PaginationInfo(queryParams.limit(), queryParams.offset(), queryParams.limit(), true);
					} else {
						paginationInfo = new PaginationInfo(queryParams.limit(), queryParams.offset(), FIRST_PAGE_LIMIT, false);
					}
					CollectorManager<TopFieldCollector, TopDocs> sharedManager = new ScoringShardsCollectorManager(luceneSort,
							LuceneUtils.safeLongToInt(paginationInfo.firstPageOffset() + paginationInfo.firstPageLimit()),
							null, LuceneUtils.totalHitsThreshold(), LuceneUtils.safeLongToInt(paginationInfo.firstPageOffset()),
							LuceneUtils.safeLongToInt(paginationInfo.firstPageLimit()));
					return new ScoredSimpleLuceneShardSearcher(sharedManager, queryParams.query(), paginationInfo);
				});
	}

}
