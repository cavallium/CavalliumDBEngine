package it.cavallium.dbengine.database.disk;

import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.search.IndexSearcher;
import org.jetbrains.annotations.Nullable;

@SuppressWarnings("unused")
public class LuceneIndexSnapshot {
	private final IndexCommit snapshot;

	private boolean initialized;
	private boolean failed;
	private boolean closed;

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
	public synchronized IndexSearcher getIndexSearcher(@Nullable Executor searchExecutor) throws IllegalStateException {
		openDirectoryIfNeeded(searchExecutor);
		return indexSearcher;
	}

	private synchronized void openDirectoryIfNeeded(@Nullable Executor searchExecutor) throws IllegalStateException {
		if (closed) {
			throw new IllegalStateException("Snapshot is closed");
		}
		if (failed) {
			throw new IllegalStateException("Snapshot failed to open");
		}
		if (!initialized) {
			try {
				var indexReader = DirectoryReader.open(snapshot);
				indexSearcher = new IndexSearcher(indexReader, searchExecutor);

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
			indexSearcher.getIndexReader().close();
			indexSearcher = null;
		}
	}
}
