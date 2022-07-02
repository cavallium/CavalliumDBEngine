package it.cavallium.dbengine.database.disk;

import static it.cavallium.dbengine.client.UninterruptibleScheduler.uninterruptibleScheduler;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.utils.SimpleResource;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
import it.cavallium.dbengine.utils.ShortNamedThreadFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

// todo: deduplicate code between Cached and Simple searcher managers
public class CachedIndexSearcherManager extends SimpleResource implements IndexSearcherManager {

	private static final Logger LOG = LogManager.getLogger(SimpleIndexSearcherManager.class);
	private static final ExecutorService SEARCH_EXECUTOR = Executors.newFixedThreadPool(
			Runtime.getRuntime().availableProcessors(),
			new ShortNamedThreadFactory("lucene-search")
					.setDaemon(true).withGroup(new ThreadGroup("lucene-search"))
	);
	private static final SearcherFactory SEARCHER_FACTORY = new ExecutorSearcherFactory(SEARCH_EXECUTOR);

	@Nullable
	private final SnapshotsManager snapshotsManager;
	private final Scheduler luceneHeavyTasksScheduler;
	private final Similarity similarity;
	private final SearcherManager searcherManager;
	private final Duration queryRefreshDebounceTime;

	private final AtomicLong activeSearchers = new AtomicLong(0);
	private final AtomicLong activeRefreshes = new AtomicLong(0);

	private final LoadingCache<LLSnapshot, Mono<LLIndexSearcher>> cachedSnapshotSearchers;
	private final Mono<LLIndexSearcher> cachedMainSearcher;
	private final Disposable refreshSubscription;

	public CachedIndexSearcherManager(IndexWriter indexWriter,
			@Nullable SnapshotsManager snapshotsManager,
			Scheduler luceneHeavyTasksScheduler,
			Similarity similarity,
			boolean applyAllDeletes,
			boolean writeAllDeletes,
			Duration queryRefreshDebounceTime) throws IOException {
		this.snapshotsManager = snapshotsManager;
		this.luceneHeavyTasksScheduler = luceneHeavyTasksScheduler;
		this.similarity = similarity;
		this.queryRefreshDebounceTime = queryRefreshDebounceTime;

		this.searcherManager = new SearcherManager(indexWriter, applyAllDeletes, writeAllDeletes, SEARCHER_FACTORY);

		refreshSubscription = luceneHeavyTasksScheduler.schedulePeriodically(() -> {
			try {
				maybeRefreshBlocking();
			} catch (Exception ex) {
				LOG.error("Failed to refresh the searcher manager", ex);
			}
		}, queryRefreshDebounceTime.toMillis(), queryRefreshDebounceTime.toMillis(), TimeUnit.MILLISECONDS);

		this.cachedSnapshotSearchers = CacheBuilder.newBuilder()
				.expireAfterWrite(queryRefreshDebounceTime)
				// Max 3 cached non-main index writers
				.maximumSize(3)
				.build(new CacheLoader<>() {
					@Override
					public Mono<LLIndexSearcher> load(@NotNull LLSnapshot snapshot) {
						return CachedIndexSearcherManager.this.generateCachedSearcher(snapshot);
					}
				});
		this.cachedMainSearcher = this.generateCachedSearcher(null);
	}

	private Mono<LLIndexSearcher> generateCachedSearcher(@Nullable LLSnapshot snapshot) {
		return Mono.fromCallable(() -> {
					if (isClosed()) {
						return null;
					}
					activeSearchers.incrementAndGet();
					try {
						IndexSearcher indexSearcher;
						boolean fromSnapshot;
						if (snapshotsManager == null || snapshot == null) {
							indexSearcher = searcherManager.acquire();
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
				})
				.subscribeOn(uninterruptibleScheduler(Schedulers.boundedElastic()))
				.publishOn(Schedulers.parallel());
	}

	private void dropCachedIndexSearcher() {
		// This shouldn't happen more than once per searcher.
		activeSearchers.decrementAndGet();
	}

	@Override
	public void maybeRefreshBlocking() throws IOException {
		try {
			activeRefreshes.incrementAndGet();
			searcherManager.maybeRefreshBlocking();
		} catch (AlreadyClosedException ignored) {

		} finally {
			activeRefreshes.decrementAndGet();
		}
	}

	@Override
	public void maybeRefresh() throws IOException {
		try {
			activeRefreshes.incrementAndGet();
			searcherManager.maybeRefresh();
		} catch (AlreadyClosedException ignored) {

		} finally {
			activeRefreshes.decrementAndGet();
		}
	}

	@Override
	public Mono<LLIndexSearcher> retrieveSearcher(@Nullable LLSnapshot snapshot) {
		if (snapshot == null) {
			return this.cachedMainSearcher;
		} else {
			return this.cachedSnapshotSearchers.getUnchecked(snapshot);
		}
	}

	@Override
	protected void onClose() {
		LOG.debug("Closing IndexSearcherManager...");
		refreshSubscription.dispose();
		LOG.debug("Closed IndexSearcherManager");
		LOG.debug("Closing refreshes...");
		long initTime = System.nanoTime();
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

	private class MainIndexSearcher extends LLIndexSearcherImpl {

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
				throw new UncheckedIOException(ex);
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
