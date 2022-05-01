package it.cavallium.dbengine.database.disk;

import static it.cavallium.dbengine.client.UninterruptibleScheduler.uninterruptibleScheduler;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.netty5.buffer.api.Send;
import it.cavallium.dbengine.database.LLSnapshot;
import java.io.IOException;
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
import org.warp.commonutils.type.ShortNamedThreadFactory;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.Empty;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class CachedIndexSearcherManager implements IndexSearcherManager {

	private static final Logger logger = LogManager.getLogger(CachedIndexSearcherManager.class);
	private final ExecutorService searchExecutor = Executors.newFixedThreadPool(
			Runtime.getRuntime().availableProcessors(),
			new ShortNamedThreadFactory("lucene-search")
					.setDaemon(true).withGroup(new ThreadGroup("lucene-search"))
	);
	private final SearcherFactory SEARCHER_FACTORY = new ExecutorSearcherFactory(searchExecutor);

	private final SnapshotsManager snapshotsManager;
	private final Scheduler luceneHeavyTasksScheduler;
	private final Similarity similarity;
	private final SearcherManager searcherManager;
	private final Duration queryRefreshDebounceTime;

	private final AtomicLong activeSearchers = new AtomicLong(0);
	private final AtomicLong activeRefreshes = new AtomicLong(0);

	private final LoadingCache<LLSnapshot, Mono<Send<LLIndexSearcher>>> cachedSnapshotSearchers;
	private final Mono<Send<LLIndexSearcher>> cachedMainSearcher;

	private final AtomicBoolean closeRequested = new AtomicBoolean();
	private final Empty<Void> closeRequestedMono = Sinks.empty();
	private final Mono<Void> closeMono;

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
						logger.error("Failed to refresh the searcher manager", ex);
					}
				})
				.subscribeOn(luceneHeavyTasksScheduler)
				.publishOn(Schedulers.parallel())
				.repeatWhen(s -> s.delayElements(queryRefreshDebounceTime))
				.takeUntilOther(closeRequestedMono.asMono())
				.doAfterTerminate(refresherClosed::tryEmitEmpty)
				.subscribe();

		this.cachedSnapshotSearchers = CacheBuilder.newBuilder()
				.expireAfterWrite(queryRefreshDebounceTime)
				// Max 3 cached non-main index writers
				.maximumSize(3)
				.build(new CacheLoader<>() {
					@Override
					public Mono<Send<LLIndexSearcher>> load(@NotNull LLSnapshot snapshot) {
						return CachedIndexSearcherManager.this.generateCachedSearcher(snapshot);
					}
				});
		this.cachedMainSearcher = this.generateCachedSearcher(null);

		this.closeMono = Mono
				.fromRunnable(() -> {
					logger.info("Closing IndexSearcherManager...");
					this.closeRequested.set(true);
					this.closeRequestedMono.tryEmitEmpty();
				})
				.then(refresherClosed.asMono())
				.then(Mono.<Void>fromRunnable(() -> {
					logger.debug("Closed IndexSearcherManager");
					logger.debug("Closing refreshes...");
					long initTime = System.nanoTime();
					while (activeRefreshes.get() > 0 && (System.nanoTime() - initTime) <= 15000000000L) {
						LockSupport.parkNanos(50000000);
					}
					logger.debug("Closed refreshes...");
					logger.debug("Closing active searchers...");
					initTime = System.nanoTime();
					while (activeSearchers.get() > 0 && (System.nanoTime() - initTime) <= 15000000000L) {
						LockSupport.parkNanos(50000000);
					}
					logger.debug("Closed active searchers");
					logger.debug("Stopping searcher executor...");
					cachedSnapshotSearchers.invalidateAll();
					cachedSnapshotSearchers.cleanUp();
					searchExecutor.shutdown();
					try {
						//noinspection BlockingMethodInNonBlockingContext
						if (!searchExecutor.awaitTermination(15, TimeUnit.SECONDS)) {
							searchExecutor.shutdownNow();
						}
					} catch (InterruptedException e) {
						logger.error("Failed to stop executor", e);
					}
					logger.debug("Stopped searcher executor");
				}).subscribeOn(uninterruptibleScheduler(Schedulers.boundedElastic())))
				.publishOn(Schedulers.parallel())
				.cache();
	}

	private Mono<Send<LLIndexSearcher>> generateCachedSearcher(@Nullable LLSnapshot snapshot) {
		return Mono.fromCallable(() -> {
					if (closeRequested.get()) {
						return null;
					}
					activeSearchers.incrementAndGet();
					IndexSearcher indexSearcher;
					boolean decRef;
					if (snapshot == null) {
						indexSearcher = searcherManager.acquire();
						decRef = true;
					} else {
						indexSearcher = snapshotsManager.resolveSnapshot(snapshot).getIndexSearcher(searchExecutor);
						decRef = false;
					}
					indexSearcher.setSimilarity(similarity);
					assert indexSearcher.getIndexReader().getRefCount() > 0;
					return new LLIndexSearcher(indexSearcher, decRef, this::dropCachedIndexSearcher).send();
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
	public Mono<Send<LLIndexSearcher>> retrieveSearcher(@Nullable LLSnapshot snapshot) {
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

}
