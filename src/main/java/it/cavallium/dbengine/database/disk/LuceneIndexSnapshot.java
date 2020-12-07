package it.cavallium.dbengine.database.disk;

import java.io.IOException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.search.IndexSearcher;

public class LuceneIndexSnapshot {
	private final IndexCommit snapshot;

	private boolean initialized;
	private boolean failed;
	private boolean closed;

	private DirectoryReader indexReader;
	private IndexSearcher indexSearcher;

	public LuceneIndexSnapshot(IndexCommit snapshot) {
		this.snapshot = snapshot;
	}

	public IndexCommit getSnapshot() {
		return snapshot;
	}

	/**
	 * Can be called only if the snapshot has not been closed
	 * @throws IllegalStateException if closed or failed
	 */
	public synchronized DirectoryReader getIndexReader() throws IllegalStateException {
		openDirectoryIfNeeded();
		return indexReader;
	}

	/**
	 * Can be called only if the snapshot has not been closed
	 * @throws IllegalStateException if closed or failed
	 */
	public synchronized IndexSearcher getIndexSearcher() throws IllegalStateException {
		openDirectoryIfNeeded();
		return indexSearcher;
	}

	private synchronized void openDirectoryIfNeeded() throws IllegalStateException {
		if (closed) {
			throw new IllegalStateException("Snapshot is closed");
		}
		if (failed) {
			throw new IllegalStateException("Snapshot failed to open");
		}
		if (!initialized) {
			try {
				indexReader = DirectoryReader.open(snapshot);
				indexSearcher = new IndexSearcher(indexReader);

				initialized = true;
			} catch (IOException e) {
				failed = true;
				throw new RuntimeException(e);
			}
		}
	}

	public synchronized void close() throws IOException {
		closed = true;

		if (initialized && !failed) {
			indexReader.close();

			indexReader = null;
			indexSearcher = null;
		}
	}
}
