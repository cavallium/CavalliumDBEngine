package it.cavallium.dbengine.database.disk;

import io.net5.buffer.api.Drop;
import io.net5.buffer.api.Owned;
import io.net5.buffer.api.internal.ResourceSupport;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;

public class LLIndexSearcher extends ResourceSupport<LLIndexSearcher, LLIndexSearcher> {

	private static final Logger logger = LogManager.getLogger(LLIndexSearcher.class);

	private static final Drop<LLIndexSearcher> DROP = new Drop<>() {
		@Override
		public void drop(LLIndexSearcher obj) {
			try {
				if (obj.onClose != null) {
					obj.onClose.run();
				}
			} catch (Throwable ex) {
				logger.error("Failed to close onClose", ex);
			}
		}

		@Override
		public Drop<LLIndexSearcher> fork() {
			return this;
		}

		@Override
		public void attach(LLIndexSearcher obj) {

		}
	};

	private IndexSearcher indexSearcher;
	private final boolean decRef;

	private Runnable onClose;

	public LLIndexSearcher(IndexSearcher indexSearcher, boolean decRef, Runnable onClose) {
		super(DROP);
		this.indexSearcher = indexSearcher;
		this.decRef = decRef;
		this.onClose = onClose;
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
		var onClose = this.onClose;
		return drop -> new LLIndexSearcher(indexSearcher, decRef, onClose);
	}

	protected void makeInaccessible() {
		this.indexSearcher = null;
		this.onClose = null;
	}

}
