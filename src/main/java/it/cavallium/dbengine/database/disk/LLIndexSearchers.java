package it.cavallium.dbengine.database.disk;

import io.netty5.buffer.api.Drop;
import io.netty5.buffer.api.Owned;
import io.netty5.buffer.api.Resource;
import io.netty5.buffer.api.Send;
import io.netty5.buffer.api.internal.ResourceSupport;
import it.cavallium.dbengine.lucene.searcher.ShardIndexSearcher;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.search.IndexSearcher;

public interface LLIndexSearchers extends Closeable {

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

	IndexReader allShards();

	class UnshardedIndexSearchers implements LLIndexSearchers {

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

		@Override
		public IndexReader allShards() {
			return indexSearcher.getIndexReader();
		}

		public IndexSearcher shard() {
			return this.shard(-1);
		}

		public LLIndexSearcher llShard() {
			return this.llShard(-1);
		}

		@Override
		public void close() throws IOException {
			indexSearcher.close();
		}
	}

	class ShardedIndexSearchers implements LLIndexSearchers {

		private final List<LLIndexSearcher> indexSearchers;
		private final List<IndexSearcher> indexSearchersVals;

		public ShardedIndexSearchers(List<LLIndexSearcher> indexSearchers) {
			var shardedIndexSearchers = new ArrayList<LLIndexSearcher>(indexSearchers.size());
			List<IndexSearcher> shardedIndexSearchersVals = new ArrayList<>(indexSearchers.size());
			for (LLIndexSearcher indexSearcher : indexSearchers) {
				shardedIndexSearchersVals.add(indexSearcher.getIndexSearcher());
			}
			shardedIndexSearchersVals = ShardIndexSearcher.create(shardedIndexSearchersVals);
			int i = 0;
			for (IndexSearcher shardedIndexSearcher : shardedIndexSearchersVals) {
				shardedIndexSearchers.add(new WrappedLLIndexSearcher(shardedIndexSearcher, indexSearchers.get(i)));
				i++;
			}
			this.indexSearchers = shardedIndexSearchers;
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
		public IndexReader allShards() {
			var irs = new IndexReader[indexSearchersVals.size()];
			for (int i = 0, s = indexSearchersVals.size(); i < s; i++) {
				irs[i] = indexSearchersVals.get(i).getIndexReader();
			}
			Object2IntOpenHashMap<IndexReader> indexes = new Object2IntOpenHashMap<>();
			for (int i = 0; i < irs.length; i++) {
				indexes.put(irs[i], i);
			}
			try {
				return new MultiReader(irs, Comparator.comparingInt(indexes::getInt), false);
			} catch (IOException ex) {
				// This shouldn't happen
				throw new UncheckedIOException(ex);
			}
		}

		@Override
		public void close() throws IOException {
			for (LLIndexSearcher indexSearcher : indexSearchers) {
				indexSearcher.close();
			}
		}

		private static class WrappedLLIndexSearcher extends LLIndexSearcher {

			private final LLIndexSearcher parent;

			public WrappedLLIndexSearcher(IndexSearcher indexSearcher, LLIndexSearcher parent) {
				super(indexSearcher, parent.getClosed());
				this.parent = parent;
			}

			@Override
			public IndexSearcher getIndexSearcher() {
				return indexSearcher;
			}

			@Override
			public IndexReader getIndexReader() {
				return indexSearcher.getIndexReader();
			}

			@Override
			protected void onClose() throws IOException {
				parent.onClose();
			}
		}
	}
}
