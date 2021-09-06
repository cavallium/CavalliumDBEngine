package it.cavallium.dbengine.database.disk;

import java.io.IOException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SearcherManager;
import org.jetbrains.annotations.Nullable;

public class CachedIndexSearcher {

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

	public void removeFromCache() throws IOException {
		synchronized (this) {
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
}
