package it.cavallium.dbengine.database.disk;

import io.net5.buffer.api.Drop;
import io.net5.buffer.api.Owned;
import io.net5.buffer.api.Resource;
import io.net5.buffer.api.Send;
import io.net5.buffer.api.internal.ResourceSupport;
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

public interface LLIndexContexts extends Resource<LLIndexContexts> {

	static LLIndexContexts of(List<Send<LLIndexContext>> indexSearchers) {
		return new ShardedIndexSearchers(indexSearchers, d -> {});
	}

	static UnshardedIndexSearchers unsharded(Send<LLIndexContext> indexSearcher) {
		return new UnshardedIndexSearchers(indexSearcher, d -> {});
	}

	Iterable<LLIndexContext> shards();

	LLIndexContext shard(int shardIndex);

	IndexReader allShards();

	class UnshardedIndexSearchers extends ResourceSupport<LLIndexContexts, UnshardedIndexSearchers>
			implements LLIndexContexts {

		private LLIndexContext indexSearcher;

		public UnshardedIndexSearchers(Send<LLIndexContext> indexSearcher, Drop<UnshardedIndexSearchers> drop) {
			super(new CloseOnDrop(drop));
			this.indexSearcher = indexSearcher.receive();
		}

		@Override
		public Iterable<LLIndexContext> shards() {
			return Collections.singleton(indexSearcher);
		}

		@Override
		public LLIndexContext shard(int shardIndex) {
			if (!isOwned()) {
				throw attachTrace(new IllegalStateException("UnshardedIndexSearchers must be owned to be used"));
			}
			if (shardIndex != -1) {
				throw new IndexOutOfBoundsException("Shard index " + shardIndex + " is invalid, this is a unsharded index");
			}
			return indexSearcher;
		}

		@Override
		public IndexReader allShards() {
			return indexSearcher.getIndexReader();
		}

		public LLIndexContext shard() {
			return this.shard(0);
		}

		@Override
		protected RuntimeException createResourceClosedException() {
			return new IllegalStateException("Closed");
		}

		@Override
		protected Owned<UnshardedIndexSearchers> prepareSend() {
			Send<LLIndexContext> indexSearcher = this.indexSearcher.send();
			this.makeInaccessible();
			return drop -> new UnshardedIndexSearchers(indexSearcher, drop);
		}

		private void makeInaccessible() {
			this.indexSearcher = null;
		}

		private static class CloseOnDrop implements Drop<UnshardedIndexSearchers> {

			private final Drop<UnshardedIndexSearchers> delegate;

			public CloseOnDrop(Drop<UnshardedIndexSearchers> drop) {
				this.delegate = drop;
			}

			@Override
			public void drop(UnshardedIndexSearchers obj) {
				try {
					if (obj.indexSearcher != null) obj.indexSearcher.close();
					delegate.drop(obj);
				} finally {
					obj.makeInaccessible();
				}
			}
		}
	}

	class ShardedIndexSearchers extends ResourceSupport<LLIndexContexts, ShardedIndexSearchers>
			implements LLIndexContexts {

		private List<LLIndexContext> indexSearchers;

		public ShardedIndexSearchers(List<Send<LLIndexContext>> indexSearchers, Drop<ShardedIndexSearchers> drop) {
			super(new CloseOnDrop(drop));
			this.indexSearchers = new ArrayList<>(indexSearchers.size());
			for (Send<LLIndexContext> indexSearcher : indexSearchers) {
				this.indexSearchers.add(indexSearcher.receive());
			}
		}

		@Override
		public Iterable<LLIndexContext> shards() {
			return Collections.unmodifiableList(indexSearchers);
		}

		@Override
		public LLIndexContext shard(int shardIndex) {
			if (!isOwned()) {
				throw attachTrace(new IllegalStateException("ShardedIndexSearchers must be owned to be used"));
			}
			if (shardIndex < 0) {
				throw new IndexOutOfBoundsException("Shard index " + shardIndex + " is invalid");
			}
			return indexSearchers.get(shardIndex);
		}

		@Override
		public IndexReader allShards() {
			var irs = new IndexReader[indexSearchers.size()];
			for (int i = 0, s = indexSearchers.size(); i < s; i++) {
				irs[i] = indexSearchers.get(i).getIndexReader();
			}
			Object2IntOpenHashMap<IndexReader> indexes = new Object2IntOpenHashMap<>();
			for (int i = 0; i < irs.length; i++) {
				indexes.put(irs[i], i);
			}
			try {
				return new MultiReader(irs, Comparator.comparingInt(indexes::getInt), true);
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
			List<Send<LLIndexContext>> indexSearchers = new ArrayList<>(this.indexSearchers.size());
			for (LLIndexContext indexSearcher : this.indexSearchers) {
				indexSearchers.add(indexSearcher.send());
			}
			this.makeInaccessible();
			return drop -> new ShardedIndexSearchers(indexSearchers, drop);
		}

		private void makeInaccessible() {
			this.indexSearchers = null;
		}

		private static class CloseOnDrop implements Drop<ShardedIndexSearchers> {

			private final Drop<ShardedIndexSearchers> delegate;

			public CloseOnDrop(Drop<ShardedIndexSearchers> drop) {
				this.delegate = drop;
			}

			@Override
			public void drop(ShardedIndexSearchers obj) {
				try {
					if (obj.indexSearchers != null) {
						for (LLIndexContext indexSearcher : obj.indexSearchers) {
							indexSearcher.close();
						}
					}
					delegate.drop(obj);
				} finally {
					obj.makeInaccessible();
				}
			}
		}
	}
}
