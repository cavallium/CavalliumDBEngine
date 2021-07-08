package it.cavallium.dbengine.lucene.searcher;

import static it.cavallium.dbengine.lucene.searcher.PaginationInfo.FIRST_PAGE_LIMIT;
import static it.cavallium.dbengine.lucene.searcher.PaginationInfo.MAX_SINGLE_SEARCH_LIMIT;

import it.cavallium.dbengine.lucene.LuceneUtils;
import reactor.core.publisher.Mono;

public class UnscoredLuceneMultiSearcher implements LuceneMultiSearcher {

	@Override
	public Mono<LuceneShardSearcher> createShardSearcher(LocalQueryParams queryParams) {
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
					UnscoredCollectorManager unsortedCollectorManager = new UnscoredCollectorManager(() -> TopDocsSearcher.getTopDocsCollector(queryParams.sort(),
							LuceneUtils.safeLongToInt(paginationInfo.firstPageOffset() + paginationInfo.firstPageLimit()),
							null,
							1000
					), queryParams.offset(), queryParams.limit(), queryParams.sort());
					return new UnscoredLuceneShardSearcher(unsortedCollectorManager, queryParams.query(), paginationInfo);
				});
	}
}
