package it.cavallium.dbengine.database.disk;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import it.cavallium.dbengine.database.LLSnapshot;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.AlreadyClosedException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.Empty;
import reactor.core.scheduler.Schedulers;

public class CachedIndexSearcherManager {

	private static final Logger logger = LoggerFactory.getLogger(CachedIndexSearcherManager.class);

	private final SnapshotsManager snapshotsManager;
	private final Similarity similarity;
	private final SearcherManager searcherManager;
	private final Duration queryRefreshDebounceTime;
	private final Phaser activeSearchers = new Phaser(1);
	private final Phaser activeRefreshes = new Phaser(1);

	private final LoadingCache<LLSnapshot, Mono<CachedIndexSearcher>> cachedSnapshotSearchers;
	private final Mono<CachedIndexSearcher> cachedMainSearcher;

	private final Empty<Void> closeRequested = Sinks.empty();
	private final Empty<Void> refresherClosed = Sinks.empty();
	private final Mono<Void> closeMono;

	public CachedIndexSearcherManager(IndexWriter indexWriter,
			SnapshotsManager snapshotsManager,
			Similarity similarity,
			boolean applyAllDeletes,
			boolean writeAllDeletes,
			Duration queryRefreshDebounceTime) throws IOException {
		this.snapshotsManager = snapshotsManager;
		this.similarity = similarity;
		this.queryRefreshDebounceTime = queryRefreshDebounceTime;

		this.searcherManager = new SearcherManager(indexWriter,
				applyAllDeletes,
				writeAllDeletes,
				new SearcherFactory()
		);

		Mono
				.fromRunnable(() -> {
					try {
						maybeRefresh();
					} catch (Exception ex) {
						logger.error("Failed to refresh the searcher manager", ex);
					}
				})
				.repeatWhen(s -> s.delayElements(queryRefreshDebounceTime, Schedulers.boundedElastic()))
				.subscribeOn(Schedulers.boundedElastic())
				.takeUntilOther(closeRequested.asMono())
				.doAfterTerminate(refresherClosed::tryEmitEmpty)
				.subscribe();

		this.cachedSnapshotSearchers = CacheBuilder.newBuilder()
				.expireAfterWrite(queryRefreshDebounceTime)
				// Max 3 cached non-main index writers
				.maximumSize(3)
				.build(new CacheLoader<>() {
					@Override
					public Mono<CachedIndexSearcher> load(@NotNull LLSnapshot snapshot) {
						return CachedIndexSearcherManager.this.generateCachedSearcher(snapshot);
					}
				});
		this.cachedMainSearcher = this.generateCachedSearcher(null);

		this.closeMono = Mono
				.fromRunnable(() -> {
					logger.info("Closing IndexSearcherManager...");
					this.closeRequested.tryEmitEmpty();
				})
				.then(refresherClosed.asMono())
				.then(Mono.<Void>fromRunnable(() -> {
					logger.info("Closed IndexSearcherManager");
					logger.info("Closing refreshes...");
					if (!activeRefreshes.isTerminated()) {
						try {
							activeRefreshes.awaitAdvanceInterruptibly(activeRefreshes.arrive(), 15, TimeUnit.SECONDS);
						} catch (Exception ex) {
							if (ex instanceof TimeoutException) {
								logger.error("Failed to terminate active refreshes: timeout");
							} else {
								logger.error("Failed to terminate active refreshes", ex);
							}
						}
					}
					logger.info("Closed refreshes...");
					logger.info("Closing active searchers...");
					if (!activeSearchers.isTerminated()) {
						try {
							activeSearchers.awaitAdvanceInterruptibly(activeSearchers.arrive(), 15, TimeUnit.SECONDS);
						} catch (Exception ex) {
							if (ex instanceof TimeoutException) {
								logger.error("Failed to terminate active searchers: timeout");
							} else {
								logger.error("Failed to terminate active searchers", ex);
							}
						}
					}
					logger.info("Closed active searchers");
					cachedSnapshotSearchers.invalidateAll();
					cachedSnapshotSearchers.cleanUp();
				})).cache();
	}

	private Mono<CachedIndexSearcher> generateCachedSearcher(@Nullable LLSnapshot snapshot) {
		return Mono.fromCallable(() -> {
					activeSearchers.register();
					IndexSearcher indexSearcher;
					SearcherManager associatedSearcherManager;
					if (snapshot == null) {
						indexSearcher = searcherManager.acquire();
						indexSearcher.setSimilarity(similarity);
						associatedSearcherManager = searcherManager;
					} else {
						indexSearcher = snapshotsManager.resolveSnapshot(snapshot).getIndexSearcher();
						associatedSearcherManager = null;
					}
					AtomicBoolean alreadyDeregistered = new AtomicBoolean(false);
					return new CachedIndexSearcher(indexSearcher, associatedSearcherManager,
							() -> {
								// This shouldn't happen more than once,
								// but I put this AtomicBoolean to be sure that this will NEVER happen more than once.
								if (alreadyDeregistered.compareAndSet(false, true)) {
									activeSearchers.arriveAndDeregister();
								} else {
									logger.error("Disposed CachedIndexSearcher twice! This is an implementation bug!");
								}
							}
					);
				})
				.cacheInvalidateWhen(indexSearcher -> Mono
								.firstWithSignal(
										this.closeRequested.asMono(),
										Mono.delay(queryRefreshDebounceTime, Schedulers.boundedElastic()).then()
								),
						indexSearcher -> {
							try {
								// Mark as removed from cache
								indexSearcher.removeFromCache();
							} catch (Exception ex) {
								logger.error("Failed to release an old cached IndexSearcher", ex);
							}
						});
	}

	public void maybeRefreshBlocking() throws IOException {
		try {
			activeRefreshes.register();
			searcherManager.maybeRefreshBlocking();
		} catch (AlreadyClosedException ignored) {

		} finally {
			activeRefreshes.arriveAndDeregister();
		}
	}

	public void maybeRefresh() throws IOException {
		try {
			activeRefreshes.register();
			searcherManager.maybeRefresh();
		} catch (AlreadyClosedException ignored) {

		} finally {
			activeRefreshes.arriveAndDeregister();
		}
	}

	public <T> Flux<T> searchMany(@Nullable LLSnapshot snapshot, Function<IndexSearcher, Flux<T>> searcherFunction) {
		return Flux.usingWhen(
				this.captureIndexSearcher(snapshot),
				indexSearcher -> searcherFunction.apply(indexSearcher.getIndexSearcher()),
				this::releaseUsedIndexSearcher
		);
	}

	public <T> Mono<T> search(@Nullable LLSnapshot snapshot, Function<IndexSearcher, Mono<T>> searcherFunction) {
		return Mono.usingWhen(
				this.captureIndexSearcher(snapshot),
				indexSearcher -> searcherFunction.apply(indexSearcher.getIndexSearcher()),
				this::releaseUsedIndexSearcher
		);
	}

	public Mono<CachedIndexSearcher> captureIndexSearcher(@Nullable LLSnapshot snapshot) {
		return this
				.retrieveCachedIndexSearcher(snapshot)
				// Increment reference count
				.doOnNext(CachedIndexSearcher::incUsage);
	}

	private Mono<CachedIndexSearcher> retrieveCachedIndexSearcher(LLSnapshot snapshot) {
		if (snapshot == null) {
			return this.cachedMainSearcher;
		} else {
			return this.cachedSnapshotSearchers.getUnchecked(snapshot);
		}
	}

	public Mono<Void> releaseUsedIndexSearcher(CachedIndexSearcher indexSearcher) {
		return Mono.fromRunnable(() -> {
			try {
				// Decrement reference count
				indexSearcher.decUsage();
			} catch (Exception ex) {
				logger.error("Failed to release an used IndexSearcher", ex);
			}
		});
	}

	public Mono<Void> close() {
		return closeMono;
	}
}
