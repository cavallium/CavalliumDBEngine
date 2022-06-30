package it.cavallium.dbengine.database.disk;

import io.netty5.buffer.api.Drop;
import io.netty5.buffer.api.Owned;
import io.netty5.buffer.api.internal.ResourceSupport;
import it.cavallium.dbengine.database.DiscardingCloseable;
import it.cavallium.dbengine.database.SafeCloseable;
import it.cavallium.dbengine.utils.SimpleResource;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;

public abstract class LLIndexSearcher extends SimpleResource implements DiscardingCloseable {

	protected static final Logger LOG = LogManager.getLogger(LLIndexSearcher.class);

	protected final IndexSearcher indexSearcher;

	public LLIndexSearcher(IndexSearcher indexSearcher) {
		super();
		this.indexSearcher = indexSearcher;
	}

	public LLIndexSearcher(IndexSearcher indexSearcher, AtomicBoolean closed) {
		super(closed);
		this.indexSearcher = indexSearcher;
	}

	public IndexReader getIndexReader() {
		ensureOpen();
		return indexSearcher.getIndexReader();
	}

	public IndexSearcher getIndexSearcher() {
		ensureOpen();
		return indexSearcher;
	}

	public AtomicBoolean getClosed() {
		return super.getClosed();
	}
}
