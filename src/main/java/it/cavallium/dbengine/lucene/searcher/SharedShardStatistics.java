package it.cavallium.dbengine.lucene.searcher;

import it.cavallium.dbengine.lucene.searcher.ShardIndexSearcher.CachedCollectionStatistics;
import it.cavallium.dbengine.lucene.searcher.ShardIndexSearcher.FieldAndShar;
import it.cavallium.dbengine.lucene.searcher.ShardIndexSearcher.TermAndShard;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.TermStatistics;

public class SharedShardStatistics {

	public final Map<FieldAndShar, CachedCollectionStatistics> collectionStatsCache = new ConcurrentHashMap<>();
	public final Map<TermAndShard, TermStatistics> termStatsCache = new ConcurrentHashMap<>();
}
