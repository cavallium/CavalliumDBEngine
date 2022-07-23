package it.cavallium.dbengine.database.disk;

import it.cavallium.dbengine.database.DiscardingCloseable;
import it.cavallium.dbengine.lucene.LuceneCloseable;
import it.cavallium.dbengine.lucene.searcher.ShardIndexSearcher;
import it.cavallium.dbengine.utils.SimpleResource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.lucene.search.IndexSearcher;

public interface LLIndexSearchers extends DiscardingCloseable {

	static LLIndexSearchers of(List<LLIndexSearcher> indexSearchers) {
		return new ShardedIndexSearchers(indexSearchers);
	}

	static UnshardedIndexSearchers unsharded(LLIndexSearcher indexSearcher) {
		return new UnshardedIndexSearchers(indexSearcher);
	}

	List<IndexSearcher> shards();

	List<LLIndexSearcher> llShards();

	IndexSearcher shard(int shardIndex);

	LLIndexSearcher llShard(int shardIndex);

	class UnshardedIndexSearchers extends SimpleResource implements LLIndexSearchers, LuceneCloseable {

		private final LLIndexSearcher indexSearcher;

		public UnshardedIndexSearchers(LLIndexSearcher indexSearcher) {
			this.indexSearcher = indexSearcher;
		}

		@Override
		public List<IndexSearcher> shards() {
			return List.of(indexSearcher.getIndexSearcher());
		}

		@Override
		public List<LLIndexSearcher> llShards() {
			return Collections.singletonList(indexSearcher);
		}

		@Override
		public IndexSearcher shard(int shardIndex) {
			if (shardIndex != -1) {
				throw new IndexOutOfBoundsException("Shard index " + shardIndex + " is invalid, this is a unsharded index");
			}
			return indexSearcher.getIndexSearcher();
		}

		@Override
		public LLIndexSearcher llShard(int shardIndex) {
			if (shardIndex != -1) {
				throw new IndexOutOfBoundsException("Shard index " + shardIndex + " is invalid, this is a unsharded index");
			}
			return indexSearcher;
		}

		public IndexSearcher shard() {
			return this.shard(-1);
		}

		public LLIndexSearcher llShard() {
			return this.llShard(-1);
		}

		@Override
		protected void onClose() {
			indexSearcher.close();
		}
	}

	class ShardedIndexSearchers extends SimpleResource implements LLIndexSearchers, LuceneCloseable {

		private final List<LLIndexSearcher> indexSearchers;
		private final List<IndexSearcher> indexSearchersVals;

		public ShardedIndexSearchers(List<LLIndexSearcher> indexSearchers) {
			List<IndexSearcher> shardedIndexSearchersVals = new ArrayList<>(indexSearchers.size());
			for (LLIndexSearcher indexSearcher : indexSearchers) {
				shardedIndexSearchersVals.add(indexSearcher.getIndexSearcher());
			}
			shardedIndexSearchersVals = ShardIndexSearcher.create(shardedIndexSearchersVals);
			this.indexSearchers = indexSearchers;
			this.indexSearchersVals = shardedIndexSearchersVals;
		}

		@Override
		public List<IndexSearcher> shards() {
			return Collections.unmodifiableList(indexSearchersVals);
		}

		@Override
		public List<LLIndexSearcher> llShards() {
			return Collections.unmodifiableList(indexSearchers);
		}

		@Override
		public IndexSearcher shard(int shardIndex) {
			if (shardIndex < 0) {
				throw new IndexOutOfBoundsException("Shard index " + shardIndex + " is invalid");
			}
			return indexSearchersVals.get(shardIndex);
		}

		@Override
		public LLIndexSearcher llShard(int shardIndex) {
			if (shardIndex < 0) {
				throw new IndexOutOfBoundsException("Shard index " + shardIndex + " is invalid");
			}
			return indexSearchers.get(shardIndex);
		}

		@Override
		protected void onClose() {
			for (LLIndexSearcher indexSearcher : indexSearchers) {
				indexSearcher.close();
			}
		}
	}
}
