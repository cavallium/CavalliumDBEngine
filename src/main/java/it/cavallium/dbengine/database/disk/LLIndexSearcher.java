package it.cavallium.dbengine.database.disk;

import io.netty5.buffer.api.Drop;
import io.netty5.buffer.api.Owned;
import io.netty5.buffer.api.internal.ResourceSupport;
import it.cavallium.dbengine.database.SafeCloseable;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;

public abstract class LLIndexSearcher implements Closeable {

	protected static final Logger LOG = LogManager.getLogger(LLIndexSearcher.class);

	protected final IndexSearcher indexSearcher;
	private final AtomicBoolean closed;

	public LLIndexSearcher(IndexSearcher indexSearcher) {
		this.indexSearcher = indexSearcher;
		this.closed = new AtomicBoolean();
	}

	public LLIndexSearcher(IndexSearcher indexSearcher, AtomicBoolean closed) {
		this.indexSearcher = indexSearcher;
		this.closed = closed;
	}

	public IndexReader getIndexReader() {
		if (closed.get()) throw new IllegalStateException("Closed");
		return indexSearcher.getIndexReader();
	}

	public IndexSearcher getIndexSearcher() {
		if (closed.get()) throw new IllegalStateException("Closed");
		return indexSearcher;
	}

	public AtomicBoolean getClosed() {
		return closed;
	}

	@Override
	public final void close() throws IOException {
		if (closed.compareAndSet(false, true)) {
			onClose();
		}
	}

	protected abstract void onClose() throws IOException;
}
