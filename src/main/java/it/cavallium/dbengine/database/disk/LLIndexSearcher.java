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

	public LLIndexSearcher() {
		super();
	}

	public LLIndexSearcher(Runnable cleanAction) {
		super(cleanAction);
	}

	public IndexSearcher getIndexSearcher() {
		ensureOpen();
		return getIndexSearcherInternal();
	}

	protected abstract IndexSearcher getIndexSearcherInternal();

	public AtomicBoolean getClosed() {
		return super.getClosed();
	}
}
