package it.cavallium.dbengine.database.disk;

import it.cavallium.dbengine.database.DiscardingCloseable;
import it.cavallium.dbengine.utils.SimpleResource;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
}
