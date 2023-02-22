package it.cavallium.dbengine.database.disk;

import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.lucene.LuceneCloseable;
import it.cavallium.dbengine.lucene.LuceneUtils;
import it.cavallium.dbengine.utils.SimpleResource;
import java.io.IOException;
import it.cavallium.dbengine.utils.DBException;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.AlreadyClosedException;
import org.jetbrains.annotations.Nullable;

// todo: deduplicate code between Cached and Simple searcher managers
public class SimpleIndexSearcherManager extends SimpleResource implements IndexSearcherManager, LuceneCloseable {

	private static final Logger LOG = LogManager.getLogger(SimpleIndexSearcherManager.class);
	private static final ExecutorService SEARCH_EXECUTOR = Executors.newFixedThreadPool(
			Runtime.getRuntime().availableProcessors(),
			new LuceneThreadFactory("lucene-search")
					.setDaemon(true).withGroup(new ThreadGroup("lucene-search"))
	);
	private static final SearcherFactory SEARCHER_FACTORY = new ExecutorSearcherFactory(SEARCH_EXECUTOR);

	@Nullable
	private final SnapshotsManager snapshotsManager;
	private final ScheduledExecutorService luceneHeavyTasksScheduler;
	private final Similarity similarity;
	private final SearcherManager searcherManager;
	private final Duration queryRefreshDebounceTime;

	private final AtomicLong activeSearchers = new AtomicLong(0);
	private final AtomicLong activeRefreshes = new AtomicLong(0);
	private final Future<?> refreshSubscription;

	public SimpleIndexSearcherManager(IndexWriter indexWriter,
			@Nullable SnapshotsManager snapshotsManager,
			ScheduledExecutorService luceneHeavyTasksScheduler,
			Similarity similarity,
			boolean applyAllDeletes,
			boolean writeAllDeletes,
			Duration queryRefreshDebounceTime) {
		this.snapshotsManager = snapshotsManager;
		this.luceneHeavyTasksScheduler = luceneHeavyTasksScheduler;
		this.similarity = similarity;
		this.queryRefreshDebounceTime = queryRefreshDebounceTime;

		try {
			this.searcherManager = new SearcherManager(indexWriter, applyAllDeletes, writeAllDeletes, SEARCHER_FACTORY);
		} catch (IOException e) {
			throw new DBException(e);
		}

		refreshSubscription = luceneHeavyTasksScheduler.scheduleAtFixedRate(() -> {
			try {
				maybeRefresh();
			} catch (Exception ex) {
				LOG.error("Failed to refresh the searcher manager", ex);
			}
		}, queryRefreshDebounceTime.toMillis(), queryRefreshDebounceTime.toMillis(), TimeUnit.MILLISECONDS);
	}

	private void dropCachedIndexSearcher() {
		// This shouldn't happen more than once per searcher.
		activeSearchers.decrementAndGet();
	}

	@Override
	public void maybeRefreshBlocking() {
		try {
			activeRefreshes.incrementAndGet();
			searcherManager.maybeRefreshBlocking();
		} catch (AlreadyClosedException ignored) {

		} catch (IOException e) {
			throw new DBException(e);
		} finally {
			activeRefreshes.decrementAndGet();
		}
	}

	@Override
	public void maybeRefresh() {
		try {
			activeRefreshes.incrementAndGet();
			searcherManager.maybeRefresh();
		} catch (AlreadyClosedException ignored) {

		} catch (IOException e) {
			throw new DBException(e);
		} finally {
			activeRefreshes.decrementAndGet();
		}
	}

	@Override
	public LLIndexSearcher retrieveSearcher(@Nullable LLSnapshot snapshot) {
		if (snapshot == null) {
			return retrieveSearcherInternal(null);
		} else {
			return retrieveSearcherInternal(snapshot);
		}
	}

	private LLIndexSearcher retrieveSearcherInternal(@Nullable LLSnapshot snapshot) {
		if (isClosed()) {
			return null;
		}
		try {
			if (snapshotsManager == null || snapshot == null) {
				return new OnDemandIndexSearcher(searcherManager, similarity);
			} else {
				activeSearchers.incrementAndGet();
				IndexSearcher indexSearcher = snapshotsManager.resolveSnapshot(snapshot).getIndexSearcher(SEARCH_EXECUTOR);
				indexSearcher.setSimilarity(similarity);
				assert indexSearcher.getIndexReader().getRefCount() > 0;
				return new SnapshotIndexSearcher(indexSearcher);
			}
		} catch (Throwable ex) {
			activeSearchers.decrementAndGet();
			throw ex;
		}
	}

	@Override
	protected void onClose() {
		LOG.debug("Closing IndexSearcherManager...");
		refreshSubscription.cancel(false);
		long initTime = System.nanoTime();
		while (!refreshSubscription.isDone() && (System.nanoTime() - initTime) <= 15000000000L) {
			LockSupport.parkNanos(50000000);
		}
		refreshSubscription.cancel(true);
		LOG.debug("Closed IndexSearcherManager");
		LOG.debug("Closing refresh tasks...");
		initTime = System.nanoTime();
		while (activeRefreshes.get() > 0 && (System.nanoTime() - initTime) <= 15000000000L) {
			LockSupport.parkNanos(50000000);
		}
		if (activeRefreshes.get() > 0) {
			LOG.warn("Some refresh tasks remained active after shutdown: {}", activeRefreshes.get());
		}
		LOG.debug("Closed refresh tasks");
		LOG.debug("Closing active searchers...");
		initTime = System.nanoTime();
		while (activeSearchers.get() > 0 && (System.nanoTime() - initTime) <= 15000000000L) {
			LockSupport.parkNanos(50000000);
		}
		if (activeSearchers.get() > 0) {
			LOG.warn("Some searchers remained active after shutdown: {}", activeSearchers.get());
		}
		LOG.debug("Closed active searchers");
		LOG.debug("Stopping searcher executor...");
		SEARCH_EXECUTOR.shutdown();
		try {
			if (!SEARCH_EXECUTOR.awaitTermination(15, TimeUnit.SECONDS)) {
				SEARCH_EXECUTOR.shutdownNow();
			}
		} catch (InterruptedException e) {
			LOG.error("Failed to stop executor", e);
		}
		LOG.debug("Stopped searcher executor");
	}

	public long getActiveSearchers() {
		return activeSearchers.get();
	}

	public long getActiveRefreshes() {
		return activeRefreshes.get();
	}

	private class MainIndexSearcher extends LLIndexSearcherImpl implements LuceneCloseable {

		public MainIndexSearcher(IndexSearcher indexSearcher) {
			super(indexSearcher, () -> releaseOnCleanup(searcherManager, indexSearcher));
		}

		private static void releaseOnCleanup(SearcherManager searcherManager, IndexSearcher indexSearcher) {
			try {
				LOG.warn("An index searcher was not closed!");
				searcherManager.release(indexSearcher);
			} catch (IOException ex) {
				LOG.error("Failed to release the index searcher during cleanup: {}", indexSearcher, ex);
			}
		}

		@Override
		public void onClose() {
			dropCachedIndexSearcher();
			try {
				searcherManager.release(indexSearcher);
			} catch (IOException ex) {
				throw new DBException(ex);
			}
		}
	}

	private class SnapshotIndexSearcher extends LLIndexSearcherImpl {

		public SnapshotIndexSearcher(IndexSearcher indexSearcher) {
			super(indexSearcher);
		}

		@Override
		public void onClose() {
			dropCachedIndexSearcher();
		}
	}

	private class OnDemandIndexSearcher extends LLIndexSearcher implements LuceneCloseable {

		private final SearcherManager searcherManager;
		private final Similarity similarity;

		private IndexSearcher indexSearcher = null;

		public OnDemandIndexSearcher(SearcherManager searcherManager,
				Similarity similarity) {
			super();
			this.searcherManager = searcherManager;
			this.similarity = similarity;
		}

		@Override
		protected IndexSearcher getIndexSearcherInternal() {
			if (indexSearcher != null) {
				return indexSearcher;
			}
			synchronized (this) {
				try {
					var indexSearcher = searcherManager.acquire();
					indexSearcher.setSimilarity(similarity);
					activeSearchers.incrementAndGet();
					this.indexSearcher = indexSearcher;
					return indexSearcher;
				} catch (IOException e) {
					throw new IllegalStateException("Failed to acquire the index searcher", e);
				}
			}
		}

		@Override
		protected void onClose() {
			try {
				synchronized (this) {
					if (indexSearcher != null) {
						dropCachedIndexSearcher();
						searcherManager.release(indexSearcher);
					}
				}
			} catch (IOException ex) {
				throw new DBException(ex);
			}
		}
	}
}
