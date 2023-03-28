package it.cavallium.dbengine.lucene.searcher;

import static it.cavallium.dbengine.utils.StreamUtils.LUCENE_POOL;
import static it.cavallium.dbengine.utils.StreamUtils.collectOn;
import static it.cavallium.dbengine.utils.StreamUtils.fastListing;

import it.cavallium.dbengine.database.disk.LLIndexSearchers;
import it.cavallium.dbengine.lucene.collector.Buckets;
import it.cavallium.dbengine.lucene.collector.DecimalBucketMultiCollectorManager;
import it.cavallium.dbengine.utils.DBException;
import java.io.IOException;
import java.util.Collection;
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

	private Buckets search(Collection<IndexSearcher> indexSearchers,
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
		return cmm.reduce(collectOn(LUCENE_POOL, indexSearchers.stream().map(indexSearcher -> {
			try {
				return cmm.search(indexSearcher);
			} catch (IOException e) {
				throw new DBException(e);
			}
		}), fastListing()));
	}
}
