package it.cavallium.dbengine.database.disk;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.IndexSearcher;

public abstract class LLIndexSearcherImpl extends LLIndexSearcher {

	protected static final Logger LOG = LogManager.getLogger(LLIndexSearcherImpl.class);

	protected final IndexSearcher indexSearcher;

	public LLIndexSearcherImpl(IndexSearcher indexSearcher) {
		super();
		this.indexSearcher = indexSearcher;
	}

	public LLIndexSearcherImpl(IndexSearcher indexSearcher, Runnable cleanAction) {
		super(cleanAction);
		this.indexSearcher = indexSearcher;
	}

	public IndexSearcher getIndexSearcherInternal() {
		return indexSearcher;
	}
}
