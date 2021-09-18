package it.cavallium.dbengine.database.disk;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.net5.buffer.api.Send;
import io.net5.buffer.api.internal.ResourceSupport;
import it.cavallium.dbengine.database.LLSnapshot;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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

public class CachedIndexSearcherManager implements IndexSearcherManager {

	private static final Logger logger = LoggerFactory.getLogger(CachedIndexSearcherManager.class);

	private final SnapshotsManager snapshotsManager;
	private final Similarity similarity;
	private final SearcherManager searcherManager;
	private final Duration queryRefreshDebounceTime;
	private final Phaser activeSearchers = new Phaser(1);
	private final Phaser activeRefreshes = new Phaser(1);

	private final LoadingCache<LLSnapshot, Mono<Send<LLIndexSearcher>>> cachedSnapshotSearchers;
	private final Mono<Send<LLIndexSearcher>> cachedMainSearcher;

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
				.subscribeOn(Schedulers.boundedElastic())
				.repeatWhen(s -> s.delayElements(queryRefreshDebounceTime))
				.takeUntilOther(closeRequested.asMono())
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

	private Mono<Send<LLIndexSearcher>> generateCachedSearcher(@Nullable LLSnapshot snapshot) {
		var onClose = this.closeRequested.asMono();
		var onQueryRefresh = Mono.delay(queryRefreshDebounceTime).then();
		var onInvalidateCache = Mono.firstWithSignal(onClose, onQueryRefresh);

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
					return new LLIndexSearcher(indexSearcher, associatedSearcherManager, this::dropCachedIndexSearcher);
				})
				.cacheInvalidateWhen(indexSearcher -> onInvalidateCache, ResourceSupport::close)
				.map(searcher -> searcher.copy(this::dropCachedIndexSearcher).send())
				.takeUntilOther(onClose)
				.doOnDiscard(ResourceSupport.class, ResourceSupport::close);
	}

	private void dropCachedIndexSearcher(LLIndexSearcher cachedIndexSearcher) {
		// This shouldn't happen more than once per searcher.
		activeSearchers.arriveAndDeregister();
	}

	@Override
	public void maybeRefreshBlocking() throws IOException {
		try {
			activeRefreshes.register();
			searcherManager.maybeRefreshBlocking();
		} catch (AlreadyClosedException ignored) {

		} finally {
			activeRefreshes.arriveAndDeregister();
		}
	}

	@Override
	public void maybeRefresh() throws IOException {
		try {
			activeRefreshes.register();
			searcherManager.maybeRefresh();
		} catch (AlreadyClosedException ignored) {

		} finally {
			activeRefreshes.arriveAndDeregister();
		}
	}

	@Override
	public <T> Flux<T> searchMany(@Nullable LLSnapshot snapshot, Function<IndexSearcher, Flux<T>> searcherFunction) {
		return Flux.usingWhen(
				this.retrieveSearcher(snapshot).map(Send::receive),
				indexSearcher -> searcherFunction.apply(indexSearcher.getIndexSearcher()),
				cachedIndexSearcher -> Mono.fromRunnable(cachedIndexSearcher::close)
		);
	}

	@Override
	public <T> Mono<T> search(@Nullable LLSnapshot snapshot, Function<IndexSearcher, Mono<T>> searcherFunction) {
		return Mono.usingWhen(
				this.retrieveSearcher(snapshot).map(Send::receive),
				indexSearcher -> searcherFunction.apply(indexSearcher.getIndexSearcher()),
				cachedIndexSearcher -> Mono.fromRunnable(cachedIndexSearcher::close)
		);
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
