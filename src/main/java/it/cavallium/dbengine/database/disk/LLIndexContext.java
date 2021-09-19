package it.cavallium.dbengine.database.disk;

import io.net5.buffer.api.Drop;
import io.net5.buffer.api.Owned;
import io.net5.buffer.api.Send;
import io.net5.buffer.api.internal.ResourceSupport;
import it.cavallium.dbengine.lucene.searcher.LLSearchTransformer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;

public class LLIndexContext extends ResourceSupport<LLIndexContext, LLIndexContext> {

	private LLIndexSearcher indexSearcher;
	private LLSearchTransformer indexQueryTransformer;

	protected LLIndexContext(Send<LLIndexSearcher> indexSearcher,
			LLSearchTransformer indexQueryTransformer,
			Drop<LLIndexContext> drop) {
		super(new CloseOnDrop(drop));
		this.indexSearcher = indexSearcher.receive();
		this.indexQueryTransformer = indexQueryTransformer;
	}

	@Override
	protected RuntimeException createResourceClosedException() {
		return new IllegalStateException("Closed");
	}

	@Override
	protected Owned<LLIndexContext> prepareSend() {
		var indexSearcher = this.indexSearcher.send();
		var indexQueryTransformer = this.indexQueryTransformer;
		makeInaccessible();
		return drop -> new LLIndexContext(indexSearcher, indexQueryTransformer, drop);
	}

	private void makeInaccessible() {
		this.indexSearcher = null;
		this.indexQueryTransformer = null;
	}

	public IndexSearcher getIndexSearcher() {
		if (!isOwned()) {
			throw new UnsupportedOperationException("Closed");
		}
		return indexSearcher.getIndexSearcher();
	}

	public IndexReader getIndexReader() {
		if (!isOwned()) {
			throw new UnsupportedOperationException("Closed");
		}
		return indexSearcher.getIndexReader();
	}

	public LLSearchTransformer getIndexQueryTransformer() {
		if (!isOwned()) {
			throw new UnsupportedOperationException("Closed");
		}
		return indexQueryTransformer;
	}

	private static class CloseOnDrop implements Drop<LLIndexContext> {

		private final Drop<LLIndexContext> delegate;

		public CloseOnDrop(Drop<LLIndexContext> drop) {
			this.delegate = drop;
		}

		@Override
		public void drop(LLIndexContext obj) {
			try {
				if (obj.indexSearcher != null) obj.indexSearcher.close();
				delegate.drop(obj);
			} finally {
				obj.makeInaccessible();
			}
		}
	}
}
