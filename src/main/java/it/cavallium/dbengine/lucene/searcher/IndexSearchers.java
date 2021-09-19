package it.cavallium.dbengine.lucene.searcher;

import io.net5.buffer.UnpooledDirectByteBuf;
import io.net5.buffer.api.Buffer;
import io.net5.buffer.api.Drop;
import io.net5.buffer.api.Owned;
import io.net5.buffer.api.Resource;
import io.net5.buffer.api.Send;
import io.net5.buffer.api.internal.ResourceSupport;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.disk.LLIndexSearcher;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.search.IndexSearcher;

public interface IndexSearchers extends Resource<IndexSearchers> {

	static IndexSearchers of(List<LLIndexSearcher> indexSearchers) {
		return new ShardedIndexSearchers(indexSearchers, d -> {});
	}

	static UnshardedIndexSearchers unsharded(Send<LLIndexSearcher> indexSearcher) {
		return new UnshardedIndexSearchers(indexSearcher, d -> {});
	}

	LLIndexSearcher shard(int shardIndex);

	class UnshardedIndexSearchers extends ResourceSupport<IndexSearchers, UnshardedIndexSearchers>
			implements IndexSearchers {

		private LLIndexSearcher indexSearcher;

		public UnshardedIndexSearchers(Send<LLIndexSearcher> indexSearcher, Drop<UnshardedIndexSearchers> drop) {
			super(new CloseOnDrop(drop));
			this.indexSearcher = indexSearcher.receive();
		}

		@Override
		public LLIndexSearcher shard(int shardIndex) {
			if (!isOwned()) {
				throw attachTrace(new IllegalStateException("UnshardedIndexSearchers must be owned to be used"));
			}
			if (shardIndex != -1) {
				throw new IndexOutOfBoundsException("Shard index " + shardIndex + " is invalid, this is a unsharded index");
			}
			return indexSearcher;
		}

		public LLIndexSearcher shard() {
			return this.shard(0);
		}

		@Override
		protected RuntimeException createResourceClosedException() {
			return new IllegalStateException("Closed");
		}

		@Override
		protected Owned<UnshardedIndexSearchers> prepareSend() {
			Send<LLIndexSearcher> indexSearcher = this.indexSearcher.send();
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

	class ShardedIndexSearchers extends ResourceSupport<IndexSearchers, ShardedIndexSearchers>
			implements IndexSearchers {

		private List<LLIndexSearcher> indexSearchers;

		public ShardedIndexSearchers(List<LLIndexSearcher> indexSearchers, Drop<ShardedIndexSearchers> drop) {
			super(new CloseOnDrop(drop));
			this.indexSearchers = indexSearchers;
		}

		@Override
		public LLIndexSearcher shard(int shardIndex) {
			if (!isOwned()) {
				throw attachTrace(new IllegalStateException("ShardedIndexSearchers must be owned to be used"));
			}
			if (shardIndex < 0) {
				throw new IndexOutOfBoundsException("Shard index " + shardIndex + " is invalid");
			}
			return indexSearchers.get(shardIndex);
		}

		@Override
		protected RuntimeException createResourceClosedException() {
			return new IllegalStateException("Closed");
		}

		@Override
		protected Owned<ShardedIndexSearchers> prepareSend() {
			List<LLIndexSearcher> indexSearchers = this.indexSearchers;
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
					delegate.drop(obj);
				} finally {
					obj.makeInaccessible();
				}
			}
		}
	}
}
