package it.cavallium.dbengine.database.disk;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import it.cavallium.dbengine.database.LLSnapshot;
import java.io.IOException;
import java.time.Duration;
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
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.Empty;
import reactor.core.scheduler.Schedulers;

public class PooledIndexSearcherManager {

	private final SnapshotsManager snapshotsManager;
	private final Similarity similarity;
	private final SearcherManager searcherManager;
	private final Duration queryRefreshDebounceTime;
	private final AtomicInteger activeSearchers = new AtomicInteger(0);

	private final LoadingCache<LLSnapshot, Mono<CachedIndexSearcher>> cachedSnapshotSearchers;
	private final Mono<CachedIndexSearcher> cachedMainSearcher;

	private final Empty<Void> closeRequested = Sinks.empty();
	private final Empty<Void> refresherClosed = Sinks.empty();

	public PooledIndexSearcherManager(IndexWriter indexWriter,
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
				.fromRunnable(this::scheduledQueryRefresh)
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
						return PooledIndexSearcherManager.this.generateCachedSearcher(snapshot);
					}
				});
		this.cachedMainSearcher = this.generateCachedSearcher(null);
	}

	private Mono<CachedIndexSearcher> generateCachedSearcher(@Nullable LLSnapshot snapshot) {
		return Mono.fromCallable(() -> {
					IndexSearcher indexSearcher;
					if (snapshot == null) {
						indexSearcher = searcherManager.acquire();
						indexSearcher.setSimilarity(similarity);
					} else {
						indexSearcher = snapshotsManager.resolveSnapshot(snapshot).getIndexSearcher();
					}
					return new CachedIndexSearcher(indexSearcher);
				})
				.cacheInvalidateWhen(indexSearcher -> Mono
								.firstWithSignal(
										this.closeRequested.asMono(),
										Mono.delay(queryRefreshDebounceTime, Schedulers.boundedElastic()).then()
								),
						indexSearcher -> {
							try {
								// Mark as removed from cache
								if (indexSearcher.removeFromCache()) {
									// Close
									try {
										if (snapshot == null) {
											searcherManager.release(indexSearcher.getIndexSearcher());
										}
									} finally {
										activeSearchers.decrementAndGet();
									}
								}
							} catch (IOException e) {
								e.printStackTrace();
							}
						});
	}

	@SuppressWarnings("unused")
	private void scheduledQueryRefresh() {
		try {
			boolean refreshStarted = searcherManager.maybeRefresh();
			// if refreshStarted == false, another thread is currently already refreshing
		} catch (AlreadyClosedException ignored) {

		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}

	public void maybeRefreshBlocking() throws IOException {
		try {
			searcherManager.maybeRefreshBlocking();
		} catch (AlreadyClosedException ignored) {

		}
	}

	public void maybeRefresh() throws IOException {
		try {
			searcherManager.maybeRefresh();
		} catch (AlreadyClosedException ignored) {

		}
	}

	public <T> Flux<T> searchMany(@Nullable LLSnapshot snapshot, Function<IndexSearcher, Flux<T>> searcherFunction) {
		return Flux.usingWhen(
				this.captureIndexSearcher(snapshot),
				indexSearcher -> searcherFunction.apply(indexSearcher.getIndexSearcher()),
				indexSearcher -> this.releaseUsedIndexSearcher(snapshot, indexSearcher)
		);
	}

	public <T> Mono<T> search(@Nullable LLSnapshot snapshot, Function<IndexSearcher, Mono<T>> searcherFunction) {
		return Mono.usingWhen(
				this.captureIndexSearcher(snapshot),
				indexSearcher -> searcherFunction.apply(indexSearcher.getIndexSearcher()),
				indexSearcher -> this.releaseUsedIndexSearcher(snapshot, indexSearcher)
		);
	}

	public Mono<CachedIndexSearcher> captureIndexSearcher(@Nullable LLSnapshot snapshot) {
		return this
				.retrieveCachedIndexSearcher(snapshot)
				// Increment reference count
				.doOnNext(indexSearcher -> {
					activeSearchers.incrementAndGet();
					indexSearcher.incUsage();
				});
	}

	private Mono<CachedIndexSearcher> retrieveCachedIndexSearcher(LLSnapshot snapshot) {
		if (snapshot == null) {
			return this.cachedMainSearcher;
		} else {
			return this.cachedSnapshotSearchers.getUnchecked(snapshot);
		}
	}

	public Mono<Void> releaseUsedIndexSearcher(@Nullable LLSnapshot snapshot, CachedIndexSearcher indexSearcher) {
		return Mono.fromRunnable(() -> {
			try {
				// Decrement reference count
				if (indexSearcher.decUsage()) {
					// Close
					try {
						if (snapshot == null) {
							searcherManager.release(indexSearcher.getIndexSearcher());
						}
					} finally {
						activeSearchers.decrementAndGet();
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		});
	}

	public Mono<Void> close() {
		return Mono
				.fromRunnable(this.closeRequested::tryEmitEmpty)
				.then(refresherClosed.asMono())
				.then(Mono.fromRunnable(() -> {
					while (activeSearchers.get() > 0) {
						// Park for 100ms
						LockSupport.parkNanos(100000000L);
					}
					cachedSnapshotSearchers.invalidateAll();
					cachedSnapshotSearchers.cleanUp();
				}));
	}
}
