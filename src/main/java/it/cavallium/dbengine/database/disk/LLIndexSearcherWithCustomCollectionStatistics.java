package it.cavallium.dbengine.database.disk;

import it.cavallium.dbengine.database.LLSearchCollectionStatisticsGetter;
import java.io.IOException;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.IndexSearcher;

public class LLIndexSearcherWithCustomCollectionStatistics extends IndexSearcher {

	private final IndexSearcher indexSearcher;
	private final LLSearchCollectionStatisticsGetter customCollectionStatisticsGetter;
	private final boolean distributedPre;
	private final long actionId;

	public LLIndexSearcherWithCustomCollectionStatistics(IndexSearcher indexSearcher,
			LLSearchCollectionStatisticsGetter customCollectionStatisticsGetter,
			boolean distributedPre,
			long actionId) {
		super(indexSearcher.getIndexReader());
		this.indexSearcher = indexSearcher;
		this.setSimilarity(indexSearcher.getSimilarity());
		this.setQueryCache(indexSearcher.getQueryCache());
		this.setQueryCachingPolicy(indexSearcher.getQueryCachingPolicy());
		this.customCollectionStatisticsGetter = customCollectionStatisticsGetter;
		this.distributedPre = distributedPre;
		this.actionId = actionId;
	}

	@Override
	public CollectionStatistics collectionStatistics(String field) throws IOException {
		return customCollectionStatisticsGetter.collectionStatistics(indexSearcher, field, distributedPre, actionId);
	}
}
