package it.cavallium.dbengine.database.disk;

import io.net5.buffer.api.Drop;
import io.net5.buffer.api.Owned;
import io.net5.buffer.api.Resource;
import io.net5.buffer.api.Send;
import io.net5.buffer.api.internal.ResourceSupport;
import it.cavallium.dbengine.database.LLEntry;
import it.cavallium.dbengine.database.LiveResourceSupport;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;

public interface LLIndexSearchers extends Resource<LLIndexSearchers> {

	static LLIndexSearchers of(List<Send<LLIndexSearcher>> indexSearchers) {
		return new ShardedIndexSearchers(indexSearchers, d -> {});
	}

	static UnshardedIndexSearchers unsharded(Send<LLIndexSearcher> indexSearcher) {
		return new UnshardedIndexSearchers(indexSearcher, d -> {});
	}

	List<IndexSearcher> shards();

	IndexSearcher shard(int shardIndex);

	IndexReader allShards();

	class UnshardedIndexSearchers extends LiveResourceSupport<LLIndexSearchers, UnshardedIndexSearchers>
			implements LLIndexSearchers {

		private LLIndexSearcher indexSearcher;

		public UnshardedIndexSearchers(Send<LLIndexSearcher> indexSearcher, Drop<UnshardedIndexSearchers> drop) {
			super(new CloseOnDrop(drop));
			this.indexSearcher = indexSearcher.receive();
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
			return drop -> new UnshardedIndexSearchers(indexSearcher, drop);
		}

		protected void makeInaccessible() {
			this.indexSearcher = null;
		}

		private static class CloseOnDrop implements Drop<UnshardedIndexSearchers> {

			private final Drop<UnshardedIndexSearchers> delegate;

			public CloseOnDrop(Drop<UnshardedIndexSearchers> drop) {
				if (drop instanceof CloseOnDrop closeOnDrop) {
					this.delegate = closeOnDrop.delegate;
				} else {
					this.delegate = drop;
				}
			}

			@Override
			public void drop(UnshardedIndexSearchers obj) {
				obj.indexSearcher.close();
				delegate.drop(obj);
			}
		}
	}

	class ShardedIndexSearchers extends LiveResourceSupport<LLIndexSearchers, ShardedIndexSearchers>
			implements LLIndexSearchers {

		private List<LLIndexSearcher> indexSearchers;
		private List<IndexSearcher> indexSearchersVals;

		public ShardedIndexSearchers(List<Send<LLIndexSearcher>> indexSearchers, Drop<ShardedIndexSearchers> drop) {
			super(new CloseOnDrop(drop));
			this.indexSearchers = new ArrayList<>(indexSearchers.size());
			this.indexSearchersVals = new ArrayList<>(indexSearchers.size());
			for (Send<LLIndexSearcher> llIndexSearcher : indexSearchers) {
				var indexSearcher = llIndexSearcher.receive();
				this.indexSearchers.add(indexSearcher);
				this.indexSearchersVals.add(indexSearcher.getIndexSearcher());
			}
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
			return drop -> new ShardedIndexSearchers(indexSearchers, drop);
		}

		protected void makeInaccessible() {
			this.indexSearchers = null;
			this.indexSearchersVals = null;
		}

		private static class CloseOnDrop implements Drop<ShardedIndexSearchers> {

			private final Drop<ShardedIndexSearchers> delegate;

			public CloseOnDrop(Drop<ShardedIndexSearchers> drop) {
				if (drop instanceof CloseOnDrop closeOnDrop) {
					this.delegate = closeOnDrop.delegate;
				} else {
					this.delegate = drop;
				}
			}

			@Override
			public void drop(ShardedIndexSearchers obj) {
				for (LLIndexSearcher indexSearcher : obj.indexSearchers) {
					indexSearcher.close();
				}
				delegate.drop(obj);
			}
		}
	}
}
