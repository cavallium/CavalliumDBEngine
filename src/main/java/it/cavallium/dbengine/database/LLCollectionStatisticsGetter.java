package it.cavallium.dbengine.database;

import java.io.IOException;
import org.apache.lucene.search.CollectionStatistics;

public interface LLCollectionStatisticsGetter {

	CollectionStatistics collectionStatistics(String field) throws IOException;
}
