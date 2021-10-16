package it.cavallium.dbengine.database.disk;

import io.net5.buffer.api.Drop;
import io.net5.buffer.api.Owned;
import io.net5.buffer.api.Resource;
import io.net5.buffer.api.Send;
import it.cavallium.dbengine.database.DatabaseResourceSupport;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.search.IndexSearcher;
import org.warp.commonutils.log.Logger;
import org.warp.commonutils.log.LoggerFactory;

public interface LLIndexSearchers extends Resource<LLIndexSearchers> {

	static LLIndexSearchers of(List<Send<LLIndexSearcher>> indexSearchers) {
		return new ShardedIndexSearchers(indexSearchers, null);
	}

	static UnshardedIndexSearchers unsharded(Send<LLIndexSearcher> indexSearcher) {
		return new UnshardedIndexSearchers(indexSearcher, null);
	}

	List<IndexSearcher> shards();

	IndexSearcher shard(int shardIndex);

	IndexReader allShards();

	class UnshardedIndexSearchers extends DatabaseResourceSupport<LLIndexSearchers, UnshardedIndexSearchers>
			implements LLIndexSearchers {

		private static final Logger logger = LoggerFactory.getLogger(UnshardedIndexSearchers.class);

		private static final Drop<UnshardedIndexSearchers> DROP = new Drop<>() {
			@Override
			public void drop(UnshardedIndexSearchers obj) {
				try {
					if (obj.indexSearcher != null) {
						obj.indexSearcher.close();
					}
				} catch (Throwable ex) {
					logger.error("Failed to close indexSearcher", ex);
				}
				try {
					if (obj.onClose != null) {
						obj.onClose.run();
					}
				} catch (Throwable ex) {
					logger.error("Failed to close onClose", ex);
				}
			}

			@Override
			public Drop<UnshardedIndexSearchers> fork() {
				return this;
			}

			@Override
			public void attach(UnshardedIndexSearchers obj) {

			}
		};

		private LLIndexSearcher indexSearcher;
		private Runnable onClose;

		public UnshardedIndexSearchers(Send<LLIndexSearcher> indexSearcher, Runnable onClose) {
			super(DROP);
			this.indexSearcher = indexSearcher.receive();
			this.onClose = onClose;
		}

		@Override
		public List<IndexSearcher> shards() {
			return List.of(indexSearcher.getIndexSearcher());
		}

		@Override
		public IndexSearcher shard(int shardIndex) {
			if (!isOwned()) {
				throw attachTrace(new IllegalStateException("UnshardedIndexSearchers must be owned to be used"));
			}
			if (shardIndex != -1) {
				throw new IndexOutOfBoundsException("Shard index " + shardIndex + " is invalid, this is a unsharded index");
			}
			return indexSearcher.getIndexSearcher();
		}

		@Override
		public IndexReader allShards() {
			return indexSearcher.getIndexReader();
		}

		public IndexSearcher shard() {
			return this.shard(-1);
		}

		@Override
		protected RuntimeException createResourceClosedException() {
			return new IllegalStateException("Closed");
		}

		@Override
		protected Owned<UnshardedIndexSearchers> prepareSend() {
			Send<LLIndexSearcher> indexSearcher = this.indexSearcher.send();
			var onClose = this.onClose;
			return drop -> {
				var instance = new UnshardedIndexSearchers(indexSearcher, onClose);
				drop.attach(instance);
				return instance;
			};
		}

		protected void makeInaccessible() {
			this.indexSearcher = null;
			this.onClose = null;
		}
	}

	class ShardedIndexSearchers extends DatabaseResourceSupport<LLIndexSearchers, ShardedIndexSearchers>
			implements LLIndexSearchers {

		private static final Logger logger = LoggerFactory.getLogger(ShardedIndexSearchers.class);

		private static final Drop<ShardedIndexSearchers> DROP = new Drop<>() {
			@Override
			public void drop(ShardedIndexSearchers obj) {
				try {
					for (LLIndexSearcher indexSearcher : obj.indexSearchers) {
						indexSearcher.close();
					}
				} catch (Throwable ex) {
					logger.error("Failed to close indexSearcher", ex);
				}
				try {
					if (obj.onClose != null) {
						obj.onClose.run();
					}
				} catch (Throwable ex) {
					logger.error("Failed to close onClose", ex);
				}
			}

			@Override
			public Drop<ShardedIndexSearchers> fork() {
				return this;
			}

			@Override
			public void attach(ShardedIndexSearchers obj) {

			}
		};

		private List<LLIndexSearcher> indexSearchers;
		private List<IndexSearcher> indexSearchersVals;
		private Runnable onClose;

		public ShardedIndexSearchers(List<Send<LLIndexSearcher>> indexSearchers, Runnable onClose) {
			super(DROP);
			this.indexSearchers = new ArrayList<>(indexSearchers.size());
			this.indexSearchersVals = new ArrayList<>(indexSearchers.size());
			for (Send<LLIndexSearcher> llIndexSearcher : indexSearchers) {
				var indexSearcher = llIndexSearcher.receive();
				this.indexSearchers.add(indexSearcher);
				this.indexSearchersVals.add(indexSearcher.getIndexSearcher());
			}
			this.onClose = onClose;
		}

		@Override
		public List<IndexSearcher> shards() {
			if (!isOwned()) {
				throw attachTrace(new IllegalStateException("ShardedIndexSearchers must be owned to be used"));
			}
			return Collections.unmodifiableList(indexSearchersVals);
		}

		@Override
		public IndexSearcher shard(int shardIndex) {
			if (!isOwned()) {
				throw attachTrace(new IllegalStateException("ShardedIndexSearchers must be owned to be used"));
			}
			if (shardIndex < 0) {
				throw new IndexOutOfBoundsException("Shard index " + shardIndex + " is invalid");
			}
			return indexSearchersVals.get(shardIndex);
		}

		@Override
		public IndexReader allShards() {
			if (!isOwned()) {
				throw attachTrace(new IllegalStateException("ShardedIndexSearchers must be owned to be used"));
			}
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
		protected RuntimeException createResourceClosedException() {
			return new IllegalStateException("Closed");
		}

		@Override
		protected Owned<ShardedIndexSearchers> prepareSend() {
			List<Send<LLIndexSearcher>> indexSearchers = new ArrayList<>(this.indexSearchers.size());
			for (LLIndexSearcher indexSearcher : this.indexSearchers) {
				indexSearchers.add(indexSearcher.send());
			}
			var onClose = this.onClose;
			return drop -> {
				var instance = new ShardedIndexSearchers(indexSearchers, onClose);
				drop.attach(instance);
				return instance;
			};
		}

		protected void makeInaccessible() {
			this.indexSearchers = null;
			this.indexSearchersVals = null;
			this.onClose = null;
		}
	}
}
