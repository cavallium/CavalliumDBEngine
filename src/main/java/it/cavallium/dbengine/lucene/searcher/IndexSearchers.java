package it.cavallium.dbengine.lucene.searcher;

import java.util.List;
import org.apache.lucene.search.IndexSearcher;

public interface IndexSearchers {

	static IndexSearchers of(List<IndexSearcher> indexSearchers) {
		return shardIndex -> {
			if (shardIndex < 0) {
				throw new IndexOutOfBoundsException("Shard index " + shardIndex + " is invalid");
			}
			return indexSearchers.get(shardIndex);
		};
	}

	static IndexSearchers unsharded(IndexSearcher indexSearcher) {
		return shardIndex -> {
			if (shardIndex != -1) {
				throw new IndexOutOfBoundsException("Shard index " + shardIndex + " is invalid, this is a unsharded index");
			}
			return indexSearcher;
		};
	}

	IndexSearcher shard(int shardIndex);
}
