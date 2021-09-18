package it.cavallium.dbengine.lucene.searcher;

import static it.cavallium.dbengine.lucene.searcher.PaginationInfo.FIRST_PAGE_LIMIT;
import static it.cavallium.dbengine.lucene.searcher.PaginationInfo.MAX_SINGLE_SEARCH_LIMIT;

import it.cavallium.dbengine.lucene.LuceneUtils;
import reactor.core.publisher.Mono;

public class UnscoredPagedLuceneMultiSearcher implements LuceneMultiSearcher {

	@Override
	public Mono<LuceneMultiSearcher> createShardSearcher(LocalQueryParams queryParams) {
		return Mono
				.fromCallable(() -> {
					if (queryParams.isScored()) {
						throw new UnsupportedOperationException("Can't use the unscored searcher to do a scored or sorted query");
					}
					PaginationInfo paginationInfo;
					if (queryParams.limit() <= MAX_SINGLE_SEARCH_LIMIT) {
						paginationInfo = new PaginationInfo(queryParams.limit(), queryParams.offset(), queryParams.limit(), true);
					} else {
						paginationInfo = new PaginationInfo(queryParams.limit(), queryParams.offset(), FIRST_PAGE_LIMIT, false);
					}
					UnscoredTopDocsCollectorManager unsortedCollectorManager = new UnscoredTopDocsCollectorManager(() -> TopDocsSearcher.getTopDocsCollector(queryParams.sort(),
							LuceneUtils.safeLongToInt(paginationInfo.firstPageOffset() + paginationInfo.firstPageLimit()),
							null,
							LuceneUtils.totalHitsThreshold(),
							!paginationInfo.forceSinglePage(),
							queryParams.isScored()
					), queryParams.offset(), queryParams.limit(), queryParams.sort());
					return new UnscoredPagedLuceneShardSearcher(unsortedCollectorManager, queryParams.query(), paginationInfo);
				});
	}
}
