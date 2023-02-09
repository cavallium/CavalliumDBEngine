package it.cavallium.dbengine.database.disk;

import it.cavallium.dbengine.database.DiscardingCloseable;
import it.cavallium.dbengine.lucene.LuceneCloseable;
import it.cavallium.dbengine.utils.SimpleResource;
import java.io.IOException;
import it.cavallium.dbengine.utils.DBException;
import java.util.concurrent.Executor;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.search.IndexSearcher;
import org.jetbrains.annotations.Nullable;

public class LuceneIndexSnapshot extends SimpleResource implements DiscardingCloseable, LuceneCloseable {
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
				this.indexReader = indexReader;
				indexSearcher = new IndexSearcher(indexReader, searchExecutor);

				initialized = true;
			} catch (IOException e) {
				failed = true;
				throw new RuntimeException(e);
			}
		}
	}

	@Override
	protected synchronized void onClose() {
		closed = true;

		if (initialized && !failed) {
			try {
				indexReader.close();
			} catch (IOException e) {
				throw new DBException(e);
			}
			indexSearcher = null;
		}
	}
}
