package it.cavallium.dbengine.lucene.searcher;

import static it.cavallium.dbengine.lucene.searcher.PaginationInfo.FIRST_PAGE_LIMIT;
import static it.cavallium.dbengine.lucene.searcher.PaginationInfo.MAX_SINGLE_SEARCH_LIMIT;

import it.cavallium.dbengine.client.query.QueryParser;
import it.cavallium.dbengine.client.query.current.data.QueryParams;
import it.cavallium.dbengine.lucene.LuceneUtils;
import java.util.Objects;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import reactor.core.publisher.Mono;

public class UnscoredLuceneMultiSearcher implements LuceneMultiSearcher {

	@Override
	public Mono<LuceneShardSearcher> createShardSearcher(LocalQueryParams queryParams) {
		return Mono
				.fromCallable(() -> {
					if (queryParams.scoreMode().needsScores()) {
						throw new UnsupportedOperationException("Can't use the unscored searcher to do a scored query");
					}
					if (queryParams.sort() != null && queryParams.sort() != Sort.RELEVANCE) {
						throw new UnsupportedOperationException("Can't use the unscored searcher to do a sorted query");
					}
					PaginationInfo paginationInfo;
					if (queryParams.limit() <= MAX_SINGLE_SEARCH_LIMIT) {
						paginationInfo = new PaginationInfo(queryParams.limit(), queryParams.offset(), queryParams.limit(), true);
					} else {
						paginationInfo = new PaginationInfo(queryParams.limit(), queryParams.offset(), FIRST_PAGE_LIMIT, true);
					}
					UnsortedCollectorManager unsortedCollectorManager = new UnsortedCollectorManager(() -> TopDocsSearcher.getTopDocsCollector(null,
							LuceneUtils.safeLongToInt(paginationInfo.firstPageOffset() + paginationInfo.firstPageLimit()),
							null,
							1000
					), queryParams.offset(), queryParams.limit());
					return new UnscoredLuceneShardSearcher(unsortedCollectorManager, queryParams.query(), paginationInfo);
				});
	}
}
