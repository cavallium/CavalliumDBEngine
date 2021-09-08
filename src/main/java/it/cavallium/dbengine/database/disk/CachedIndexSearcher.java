package it.cavallium.dbengine.database.disk;

import java.io.IOException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SearcherManager;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CachedIndexSearcher {

	private static final Logger logger = LoggerFactory.getLogger(CachedIndexSearcher.class);

	private final IndexSearcher indexSearcher;
	private final SearcherManager associatedSearcherManager;
	private final Runnable afterFinalization;
	private boolean inCache = true;
	private int usages = 0;

	public CachedIndexSearcher(IndexSearcher indexSearcher,
			@Nullable SearcherManager associatedSearcherManager,
			@Nullable Runnable afterFinalization) {
		this.indexSearcher = indexSearcher;
		this.associatedSearcherManager = associatedSearcherManager;
		this.afterFinalization = afterFinalization;
	}

	public void incUsage() {
		synchronized (this) {
			usages++;
		}
	}

	public void decUsage() throws IOException {
		synchronized (this) {
			if (usages > 0) {
				usages--;
				if (mustClose()) {
					try {
						close();
					} finally {
						if (afterFinalization != null) afterFinalization.run();
					}
				}
			}
		}
	}

	public void removeFromCache() throws IOException {
		synchronized (this) {
			if (inCache) {
				inCache = false;
				if (mustClose()) {
					try {
						close();
					} finally {
						if (afterFinalization != null) afterFinalization.run();
					}
				}
			}
		}
	}

	private void close() throws IOException {
		if (associatedSearcherManager != null) {
			associatedSearcherManager.release(indexSearcher);
		}
	}

	private boolean mustClose() {
		return !this.inCache && this.usages == 0;
	}

	public IndexReader getIndexReader() {
		return indexSearcher.getIndexReader();
	}

	public IndexSearcher getIndexSearcher() {
		return indexSearcher;
	}

	@SuppressWarnings("deprecation")
	@Override
	protected void finalize() throws Throwable {
		boolean failedToRelease = false;
		if (usages > 0) {
			failedToRelease = true;
			logger.error("A cached index searcher has been garbage collected, but "
					+ usages + " usages have not been released");
		}
		if (inCache) {
			failedToRelease = true;
			logger.error("A cached index searcher has been garbage collected, but it's marked"
					+ " as still actively cached");
		}
		if (failedToRelease) {
			try {
				this.close();
			} catch (Throwable ex) {
				logger.warn("Error when closing cached index searcher", ex);
			}
		}
		super.finalize();
	}
}
