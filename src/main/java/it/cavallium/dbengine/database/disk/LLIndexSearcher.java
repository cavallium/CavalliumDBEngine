package it.cavallium.dbengine.database.disk;

import io.net5.buffer.api.Drop;
import io.net5.buffer.api.Owned;
import io.net5.buffer.api.internal.ResourceSupport;
import java.io.IOException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SearcherManager;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LLIndexSearcher extends ResourceSupport<LLIndexSearcher, LLIndexSearcher> {

	private static final Logger logger = LoggerFactory.getLogger(LLIndexSearcher.class);

	private IndexSearcher indexSearcher;
	private SearcherManager associatedSearcherManager;

	public LLIndexSearcher(IndexSearcher indexSearcher,
			@Nullable SearcherManager associatedSearcherManager,
			Drop<LLIndexSearcher> drop) {
		super(new LLIndexSearcher.CloseOnDrop(drop));
		this.indexSearcher = indexSearcher;
		this.associatedSearcherManager = associatedSearcherManager;
	}

	public IndexReader getIndexReader() {
		if (!isOwned()) {
			throw attachTrace(new IllegalStateException("CachedIndexSearcher must be owned to be used"));
		}
		return indexSearcher.getIndexReader();
	}

	public IndexSearcher getIndexSearcher() {
		if (!isOwned()) {
			throw attachTrace(new IllegalStateException("CachedIndexSearcher must be owned to be used"));
		}
		return indexSearcher;
	}

	public LLIndexSearcher copy(Drop<LLIndexSearcher> drop) {
		if (!isOwned()) {
			throw attachTrace(new IllegalStateException("CachedIndexSearcher must be owned to be used"));
		}
		var copyIndexSearcher = this.indexSearcher;
		if (associatedSearcherManager != null) {
			copyIndexSearcher.getIndexReader().incRef();
		}
		return new LLIndexSearcher(copyIndexSearcher, associatedSearcherManager, new CloseOnDrop(drop));
	}

	@Override
	protected RuntimeException createResourceClosedException() {
		return new IllegalStateException("Closed");
	}

	@Override
	protected Owned<LLIndexSearcher> prepareSend() {
		var indexSearcher = this.indexSearcher;
		var associatedSearcherManager = this.associatedSearcherManager;
		makeInaccessible();
		return drop -> new LLIndexSearcher(indexSearcher, associatedSearcherManager, drop);
	}

	private void makeInaccessible() {
		this.indexSearcher = null;
		this.associatedSearcherManager = null;
	}

	private static class CloseOnDrop implements Drop<LLIndexSearcher> {

		private final Drop<LLIndexSearcher> delegate;

		public CloseOnDrop(Drop<LLIndexSearcher> drop) {
			this.delegate = drop;
		}

		@Override
		public void drop(LLIndexSearcher obj) {
			try {
				if (obj.associatedSearcherManager != null) {
					if (obj.indexSearcher.getIndexReader().getRefCount() > 0) {
						obj.associatedSearcherManager.release(obj.indexSearcher);
					}
				}
				delegate.drop(obj);
			} catch (IOException e) {
				logger.error("Failed to drop CachedIndexSearcher", e);
			} finally {
				obj.makeInaccessible();
			}
		}
	}
}
