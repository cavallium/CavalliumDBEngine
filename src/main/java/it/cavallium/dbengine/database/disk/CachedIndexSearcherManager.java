package it.cavallium.dbengine.database.disk;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.lucene.LuceneCloseable;
import it.cavallium.dbengine.utils.SimpleResource;
import java.io.IOException;
import it.cavallium.dbengine.utils.DBException;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.AlreadyClosedException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

// todo: deduplicate code between Cached and Simple searcher managers
public class CachedIndexSearcherManager extends SimpleResource implements IndexSearcherManager, LuceneCloseable {

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

	private final LoadingCache<LLSnapshot, LLIndexSearcher> cachedSnapshotSearchers;
	private final ScheduledFuture<?> refreshSubscription;

	public CachedIndexSearcherManager(IndexWriter indexWriter,
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
				},
				queryRefreshDebounceTime.toMillis(),
				queryRefreshDebounceTime.toMillis(),
				TimeUnit.MILLISECONDS
		);

		this.cachedSnapshotSearchers = CacheBuilder.newBuilder()
				.expireAfterWrite(queryRefreshDebounceTime)
				// Max 3 cached non-main index writers
				.maximumSize(3)
				.build(new CacheLoader<>() {
					@Override
					public LLIndexSearcher load(@NotNull LLSnapshot snapshot) {
						return CachedIndexSearcherManager.this.generateCachedSearcher(snapshot);
					}
				});
	}

	private LLIndexSearcher generateCachedSearcher(@Nullable LLSnapshot snapshot) {
		if (isClosed()) {
			return null;
		}
		activeSearchers.incrementAndGet();
		try {
			IndexSearcher indexSearcher;
			boolean fromSnapshot;
			if (snapshotsManager == null || snapshot == null) {
				try {
					indexSearcher = searcherManager.acquire();
				} catch (IOException ex) {
					throw new DBException(ex);
				}
				fromSnapshot = false;
			} else {
				indexSearcher = snapshotsManager.resolveSnapshot(snapshot).getIndexSearcher(SEARCH_EXECUTOR);
				fromSnapshot = true;
			}
			indexSearcher.setSimilarity(similarity);
			assert indexSearcher.getIndexReader().getRefCount() > 0;
			LLIndexSearcher llIndexSearcher;
			if (fromSnapshot) {
				llIndexSearcher = new SnapshotIndexSearcher(indexSearcher);
			} else {
				llIndexSearcher = new MainIndexSearcher(indexSearcher, searcherManager);
			}
			return llIndexSearcher;
		} catch (Throwable ex) {
			activeSearchers.decrementAndGet();
			throw ex;
		}
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
			return this.generateCachedSearcher(null);
		} else {
			return this.cachedSnapshotSearchers.getUnchecked(snapshot);
		}
	}

	@Override
	protected void onClose() {
		LOG.debug("Closing IndexSearcherManager...");
		long initTime = System.nanoTime();
		refreshSubscription.cancel(false);
		while (!refreshSubscription.isDone() && (System.nanoTime() - initTime) <= 240000000000L) {
			LockSupport.parkNanos(50000000);
		}
		refreshSubscription.cancel(true);
		LOG.debug("Closed IndexSearcherManager");
		LOG.debug("Closing refreshes...");
		initTime = System.nanoTime();
		while (activeRefreshes.get() > 0 && (System.nanoTime() - initTime) <= 15000000000L) {
			LockSupport.parkNanos(50000000);
		}
		LOG.debug("Closed refreshes...");
		LOG.debug("Closing active searchers...");
		initTime = System.nanoTime();
		while (activeSearchers.get() > 0 && (System.nanoTime() - initTime) <= 15000000000L) {
			LockSupport.parkNanos(50000000);
		}
		LOG.debug("Closed active searchers");
		LOG.debug("Stopping searcher executor...");
		cachedSnapshotSearchers.invalidateAll();
		cachedSnapshotSearchers.cleanUp();
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

		public MainIndexSearcher(IndexSearcher indexSearcher, SearcherManager searcherManager) {
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
}
