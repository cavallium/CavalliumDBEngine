package it.cavallium.dbengine.lucene.searcher;

import com.google.common.collect.Streams;
import it.cavallium.dbengine.database.disk.LLIndexSearchers;
import it.cavallium.dbengine.lucene.collector.Buckets;
import it.cavallium.dbengine.lucene.collector.DecimalBucketMultiCollectorManager;
import java.io.IOException;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class DecimalBucketMultiSearcher {

	protected static final Logger logger = LogManager.getLogger(DecimalBucketMultiSearcher.class);

	public Buckets collectMulti(LLIndexSearchers indexSearchers,
			BucketParams bucketParams,
			@NotNull List<Query> queries,
			@Nullable Query normalizationQuery) {
		try {
			// Search results
			return this.search(indexSearchers.shards(), bucketParams, queries, normalizationQuery);
		} finally {
			indexSearchers.close();
		}
	}

	private Buckets search(Iterable<IndexSearcher> indexSearchers,
			BucketParams bucketParams,
			@NotNull List<Query> queries,
			@Nullable Query normalizationQuery) {
		var cmm = new DecimalBucketMultiCollectorManager(bucketParams.min(),
				bucketParams.max(),
				bucketParams.buckets(),
				bucketParams.bucketFieldName(),
				bucketParams.valueSource(),
				queries,
				normalizationQuery,
				bucketParams.collectionRate(),
				bucketParams.sampleSize()
		);
		return cmm.reduce(Streams.stream(indexSearchers).parallel().map(shard -> {
			try {
				return cmm.search(shard);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}).toList());
	}
}
