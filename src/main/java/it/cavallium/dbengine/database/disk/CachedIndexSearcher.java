package it.cavallium.dbengine.database.disk;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;

public class CachedIndexSearcher {

	private final IndexSearcher indexSearcher;
	private boolean inCache = true;
	private int usages = 0;

	public CachedIndexSearcher(IndexSearcher indexSearcher) {
		this.indexSearcher = indexSearcher;
	}

	public void incUsage() {
		synchronized (this) {
			usages++;
		}
	}

	/**
	 *
	 * @return true if closed
	 */
	public boolean decUsage() {
		synchronized (this) {
			usages--;
			return isClosed();
		}
	}

	/**
	 *
	 * @return true if closed
	 */
	public boolean removeFromCache() {
		synchronized (this) {
			inCache = false;
			return isClosed();
		}
	}

	private boolean isClosed() {
		return this.inCache || this.usages > 0;
	}

	public IndexReader getIndexReader() {
		return indexSearcher.getIndexReader();
	}

	public IndexSearcher getIndexSearcher() {
		return indexSearcher;
	}
}
