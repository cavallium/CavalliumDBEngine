package it.cavallium.dbengine.database.disk;

import io.net5.buffer.api.Drop;
import io.net5.buffer.api.Owned;
import io.net5.buffer.api.internal.ResourceSupport;
import it.cavallium.dbengine.database.LiveResourceSupport;
import java.io.IOException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SearcherManager;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LLIndexSearcher extends LiveResourceSupport<LLIndexSearcher, LLIndexSearcher> {

	private IndexSearcher indexSearcher;

	public LLIndexSearcher(IndexSearcher indexSearcher, Drop<LLIndexSearcher> drop) {
		super(drop);
		this.indexSearcher = indexSearcher;
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
		return drop -> new LLIndexSearcher(indexSearcher, drop);
	}

	protected void makeInaccessible() {
		this.indexSearcher = null;
	}
}
