package it.cavallium.dbengine.database.disk;

import io.net5.buffer.api.Drop;
import io.net5.buffer.api.Owned;
import it.cavallium.dbengine.database.LiveResourceSupport;
import java.io.IOException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LLIndexSearcher extends LiveResourceSupport<LLIndexSearcher, LLIndexSearcher> {

	private static final Logger logger = LoggerFactory.getLogger(LLIndexSearcher.class);

	private IndexSearcher indexSearcher;
	private final boolean decRef;

	public LLIndexSearcher(IndexSearcher indexSearcher, boolean decRef, Drop<LLIndexSearcher> drop) {
		super(new CloseOnDrop(drop));
		this.indexSearcher = indexSearcher;
		this.decRef = decRef;
	}

	public IndexReader getIndexReader() {
		if (!isOwned()) {
			throw attachTrace(new IllegalStateException("LLIndexSearcher must be owned to be used"));
		}
		return indexSearcher.getIndexReader();
	}

	public IndexSearcher getIndexSearcher() {
		if (!isOwned()) {
			throw attachTrace(new IllegalStateException("LLIndexSearcher must be owned to be used"));
		}
		return indexSearcher;
	}

	@Override
	protected RuntimeException createResourceClosedException() {
		return new IllegalStateException("Closed");
	}

	@Override
	protected Owned<LLIndexSearcher> prepareSend() {
		var indexSearcher = this.indexSearcher;
		return drop -> new LLIndexSearcher(indexSearcher, decRef, drop);
	}

	protected void makeInaccessible() {
		this.indexSearcher = null;
	}

	private static class CloseOnDrop implements Drop<LLIndexSearcher> {

		private final Drop<LLIndexSearcher> delegate;

		public CloseOnDrop(Drop<LLIndexSearcher> drop) {
			if (drop instanceof CloseOnDrop closeOnDrop) {
				this.delegate = closeOnDrop.delegate;
			} else {
				this.delegate = drop;
			}
		}

		@Override
		public void drop(LLIndexSearcher obj) {
			try {
				obj.indexSearcher.getIndexReader().decRef();
			} catch (IOException ex) {
				logger.error("Failed to drop IndexReader", ex);
			}
			delegate.drop(obj);
		}
	}
}
