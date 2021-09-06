package it.cavallium.dbengine.database.disk;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import it.cavallium.dbengine.database.LLSnapshot;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.similarities.Similarity;
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

	private final LoadingCache<LLSnapshot, Mono<IndexSearcher>> cachedSnapshotSearchers;
	private final Mono<IndexSearcher> cachedMainSearcher;

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
					public Mono<IndexSearcher> load(@NotNull LLSnapshot snapshot) {
						return PooledIndexSearcherManager.this.generateCachedSearcher(snapshot);
					}
				});
		this.cachedMainSearcher = this.generateCachedSearcher(null);
	}

	private Mono<IndexSearcher> generateCachedSearcher(@Nullable LLSnapshot snapshot) {
		return Mono.fromCallable(() -> {
					IndexSearcher indexSearcher;
					if (snapshot == null) {
						indexSearcher = searcherManager.acquire();
						indexSearcher.setSimilarity(similarity);
					} else {
						indexSearcher = snapshotsManager.resolveSnapshot(snapshot).getIndexSearcher();
					}
					return indexSearcher;
				})
				.cacheInvalidateWhen(indexSearcher -> Mono
								.firstWithSignal(
										this.closeRequested.asMono(),
										Mono.delay(queryRefreshDebounceTime, Schedulers.boundedElastic()).then()
								),
						indexSearcher -> {
							try {
								//noinspection SynchronizationOnLocalVariableOrMethodParameter
								synchronized (indexSearcher) {
									// Close
									if (indexSearcher.getIndexReader().getRefCount() <= 0) {
										if (snapshot == null) {
											searcherManager.release(indexSearcher);
										}
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
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}

	public void maybeRefreshBlocking() throws IOException {
		searcherManager.maybeRefreshBlocking();
	}

	public void maybeRefresh() throws IOException {
		searcherManager.maybeRefresh();
	}

	public <T> Flux<T> searchMany(@Nullable LLSnapshot snapshot, Function<IndexSearcher, Flux<T>> searcherFunction) {
		return Flux.usingWhen(
				this.captureIndexSearcher(snapshot),
				searcherFunction,
				indexSearcher -> this.releaseUsedIndexSearcher(snapshot, indexSearcher)
		);
	}

	public <T> Mono<T> search(@Nullable LLSnapshot snapshot, Function<IndexSearcher, Mono<T>> searcherFunction) {
		return Mono.usingWhen(
				this.captureIndexSearcher(snapshot),
				searcherFunction,
				indexSearcher -> this.releaseUsedIndexSearcher(snapshot, indexSearcher)
		);
	}

	public Mono<IndexSearcher> captureIndexSearcher(@Nullable LLSnapshot snapshot) {
		return this
				.retrieveCachedIndexSearcher(snapshot)
				// Increment reference count
				.doOnNext(indexSearcher -> indexSearcher.getIndexReader().incRef());
	}

	private Mono<IndexSearcher> retrieveCachedIndexSearcher(LLSnapshot snapshot) {
		if (snapshot == null) {
			return this.cachedMainSearcher;
		} else {
			return this.cachedSnapshotSearchers.getUnchecked(snapshot);
		}
	}

	public Mono<Void> releaseUsedIndexSearcher(@Nullable LLSnapshot snapshot, IndexSearcher indexSearcher) {
		return Mono.fromRunnable(() -> {
			try {
				synchronized (indexSearcher) {
					// Decrement reference count
					indexSearcher.getIndexReader().decRef();
					// Close
					if (indexSearcher.getIndexReader().getRefCount() <= 0) {
						if (snapshot == null) {
							searcherManager.release(indexSearcher);
						}
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
					cachedSnapshotSearchers.invalidateAll();
					cachedSnapshotSearchers.cleanUp();
				}));
	}
}
