package it.cavallium.dbengine.database.disk;

import io.net5.buffer.api.Drop;
import io.net5.buffer.api.Owned;
import it.cavallium.dbengine.database.LiveResourceSupport;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;

public class LLIndexSearcher extends LiveResourceSupport<LLIndexSearcher, LLIndexSearcher> {

	private IndexSearcher indexSearcher;
	private final boolean decRef;

	public LLIndexSearcher(IndexSearcher indexSearcher, boolean decRef, Drop<LLIndexSearcher> drop) {
		super(drop);
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

}
