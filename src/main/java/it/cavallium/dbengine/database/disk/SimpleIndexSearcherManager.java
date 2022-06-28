package it.cavallium.dbengine.database.disk;

import static it.cavallium.dbengine.client.UninterruptibleScheduler.uninterruptibleScheduler;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.utils.SimpleResource;
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

// todo: deduplicate code between Cached and Simple searcher managers
public class SimpleIndexSearcherManager implements IndexSearcherManager {

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

	private Mono<LLIndexSearcher> noSnapshotSearcherMono;

	private final AtomicLong activeSearchers = new AtomicLong(0);
	private final AtomicLong activeRefreshes = new AtomicLong(0);

	private final AtomicBoolean closeRequested = new AtomicBoolean();
	private final Empty<Void> closeRequestedMono = Sinks.empty();
	private final Mono<Void> closeMono;

	public SimpleIndexSearcherManager(IndexWriter indexWriter,
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

		Empty<Void> refresherClosed = Sinks.empty();
		var refreshSubscription = Mono
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

		this.closeMono = Mono
				.fromRunnable(() -> {
					LOG.debug("Closing IndexSearcherManager...");
					this.closeRequested.set(true);
					this.closeRequestedMono.tryEmitEmpty();
					refreshSubscription.dispose();
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
		this.noSnapshotSearcherMono = retrieveSearcherInternal(null);
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
			return noSnapshotSearcherMono;
		} else {
			return retrieveSearcherInternal(snapshot);
		}
	}

	private Mono<LLIndexSearcher> retrieveSearcherInternal(@Nullable LLSnapshot snapshot) {
		return Mono.fromCallable(() -> {
					if (closeRequested.get()) {
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
						SimpleResource.CLEANER.register(llIndexSearcher, () -> {
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
				})
				.subscribeOn(uninterruptibleScheduler(Schedulers.boundedElastic()))
				.publishOn(Schedulers.parallel());
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
