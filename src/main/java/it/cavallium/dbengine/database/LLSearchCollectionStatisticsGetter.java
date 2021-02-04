package it.cavallium.dbengine.database;

import java.io.IOException;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.IndexSearcher;

public interface LLSearchCollectionStatisticsGetter {

	CollectionStatistics collectionStatistics(IndexSearcher indexSearcher,
			String field,
			boolean distributedPre,
			long actionId) throws IOException;
}
