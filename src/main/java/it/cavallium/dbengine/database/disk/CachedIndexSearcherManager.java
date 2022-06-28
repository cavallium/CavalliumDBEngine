package it.cavallium.dbengine.database.disk;

import static it.cavallium.dbengine.client.UninterruptibleScheduler.uninterruptibleScheduler;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.LLUtils;
import java.io.IOException;
import java.lang.ref.Cleaner;
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
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.Empty;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class CachedIndexSearcherManager implements IndexSearcherManager {

	private static final Logger LOG = LogManager.getLogger(CachedIndexSearcherManager.class);
	private static final ExecutorService SEARCH_EXECUTOR = Executors.newFixedThreadPool(
			Runtime.getRuntime().availableProcessors(),
			new ShortNamedThreadFactory("lucene-search")
					.setDaemon(true).withGroup(new ThreadGroup("lucene-search"))
	);
	private static final SearcherFactory SEARCHER_FACTORY = new ExecutorSearcherFactory(SEARCH_EXECUTOR);

	private final SnapshotsManager snapshotsManager;
	private final Scheduler luceneHeavyTasksScheduler;
	private final Similarity similarity;
	private final SearcherManager searcherManager;
	private final Duration queryRefreshDebounceTime;

	private final AtomicLong activeSearchers = new AtomicLong(0);
	private final AtomicLong activeRefreshes = new AtomicLong(0);

	private final LoadingCache<LLSnapshot, Mono<LLIndexSearcher>> cachedSnapshotSearchers;
	private final Mono<LLIndexSearcher> cachedMainSearcher;

	private final AtomicBoolean closeRequested = new AtomicBoolean();
	private final Empty<Void> closeRequestedMono = Sinks.empty();
	private final Mono<Void> closeMono;

	private static final Cleaner CLEANER = Cleaner.create();

	public CachedIndexSearcherManager(IndexWriter indexWriter,
			SnapshotsManager snapshotsManager,
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

		Empty<Void> refresherClosed = Sinks.empty();
		Mono
				.fromRunnable(() -> {
					try {
						maybeRefresh();
					} catch (Exception ex) {
						LOG.error("Failed to refresh the searcher manager", ex);
					}
				})
				.subscribeOn(luceneHeavyTasksScheduler)
				.publishOn(Schedulers.parallel())
				.repeatWhen(s -> s.delayElements(queryRefreshDebounceTime))
				.takeUntilOther(closeRequestedMono.asMono())
				.doAfterTerminate(refresherClosed::tryEmitEmpty)
				.transform(LLUtils::handleDiscard)
				.subscribe();

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

		this.closeMono = Mono
				.fromRunnable(() -> {
					LOG.debug("Closing IndexSearcherManager...");
					this.closeRequested.set(true);
					this.closeRequestedMono.tryEmitEmpty();
				})
				.then(refresherClosed.asMono())
				.then(Mono.<Void>fromRunnable(() -> {
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
						//noinspection BlockingMethodInNonBlockingContext
						if (!SEARCH_EXECUTOR.awaitTermination(15, TimeUnit.SECONDS)) {
							SEARCH_EXECUTOR.shutdownNow();
						}
					} catch (InterruptedException e) {
						LOG.error("Failed to stop executor", e);
					}
					LOG.debug("Stopped searcher executor");
				}).subscribeOn(uninterruptibleScheduler(Schedulers.boundedElastic())))
				.publishOn(Schedulers.parallel())
				.cache();
	}

	private Mono<LLIndexSearcher> generateCachedSearcher(@Nullable LLSnapshot snapshot) {
		return Mono.fromCallable(() -> {
			if (closeRequested.get()) {
				return null;
			}
			activeSearchers.incrementAndGet();
			try {
				IndexSearcher indexSearcher;
				boolean fromSnapshot;
				if (snapshot == null) {
					indexSearcher = searcherManager.acquire();
					fromSnapshot = false;
				} else {
					//noinspection resource
					indexSearcher = snapshotsManager.resolveSnapshot(snapshot).getIndexSearcher(SEARCH_EXECUTOR);
					fromSnapshot = true;
				}
				indexSearcher.setSimilarity(similarity);
				assert indexSearcher.getIndexReader().getRefCount() > 0;
				var closed = new AtomicBoolean();
				LLIndexSearcher llIndexSearcher;
				if (fromSnapshot) {
					llIndexSearcher = new SnapshotIndexSearcher(indexSearcher, closed);
				} else {
					llIndexSearcher = new MainIndexSearcher(indexSearcher, closed);
				}
				CLEANER.register(llIndexSearcher, () -> {
					if (closed.compareAndSet(false, true)) {
						LOG.warn("An index searcher was not closed!");
						if (!fromSnapshot) {
							try {
								searcherManager.release(indexSearcher);
							} catch (IOException e) {
								LOG.error("Failed to release the index searcher", e);
							}
						}
					}
				});
				return llIndexSearcher;
			} catch (Throwable ex) {
				activeSearchers.decrementAndGet();
				throw ex;
			}
		});
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
	public Mono<Void> close() {
		return closeMono;
	}

	public long getActiveSearchers() {
		return activeSearchers.get();
	}

	public long getActiveRefreshes() {
		return activeRefreshes.get();
	}

	private class MainIndexSearcher extends LLIndexSearcher {

		public MainIndexSearcher(IndexSearcher indexSearcher, AtomicBoolean released) {
			super(indexSearcher, released);
		}

		@Override
		public void onClose() throws IOException {
			dropCachedIndexSearcher();
			if (getClosed().compareAndSet(false, true)) {
				searcherManager.release(indexSearcher);
			}
		}
	}

	private class SnapshotIndexSearcher extends LLIndexSearcher {

		public SnapshotIndexSearcher(IndexSearcher indexSearcher,
				AtomicBoolean closed) {
			super(indexSearcher, closed);
		}

		@Override
		public void onClose() throws IOException {
			dropCachedIndexSearcher();
		}
	}
}
