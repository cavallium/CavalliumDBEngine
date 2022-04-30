package it.cavallium.dbengine.database.disk;

import static it.cavallium.dbengine.client.UninterruptibleScheduler.uninterruptibleScheduler;
import static it.cavallium.dbengine.database.LLUtils.MARKER_LUCENE;
import static it.cavallium.dbengine.database.LLUtils.toDocument;
import static it.cavallium.dbengine.database.LLUtils.toFields;
import static it.cavallium.dbengine.lucene.searcher.GlobalQueryRewrite.NO_REWRITE;
import static reactor.core.scheduler.Schedulers.DEFAULT_BOUNDED_ELASTIC_QUEUESIZE;
import static reactor.core.scheduler.Schedulers.DEFAULT_BOUNDED_ELASTIC_SIZE;

import com.google.common.collect.Multimap;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.netty5.buffer.api.Send;
import it.cavallium.dbengine.client.query.QueryParser;
import it.cavallium.dbengine.client.query.current.data.Query;
import it.cavallium.dbengine.client.query.current.data.QueryParams;
import it.cavallium.dbengine.database.LLIndexRequest;
import it.cavallium.dbengine.database.LLLuceneIndex;
import it.cavallium.dbengine.database.LLSearchResultShard;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.LLSoftUpdateDocument;
import it.cavallium.dbengine.database.LLTerm;
import it.cavallium.dbengine.database.LLUpdateDocument;
import it.cavallium.dbengine.database.LLUpdateFields;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.lucene.LuceneHacks;
import it.cavallium.dbengine.lucene.LuceneRocksDBManager;
import it.cavallium.dbengine.lucene.LuceneUtils;
import it.cavallium.dbengine.lucene.collector.Buckets;
import it.cavallium.dbengine.lucene.directory.Lucene91CodecWithNoFieldCompression;
import it.cavallium.dbengine.lucene.mlt.MoreLikeThisTransformer;
import it.cavallium.dbengine.lucene.searcher.AdaptiveLocalSearcher;
import it.cavallium.dbengine.lucene.searcher.BucketParams;
import it.cavallium.dbengine.lucene.searcher.DecimalBucketMultiSearcher;
import it.cavallium.dbengine.lucene.searcher.LocalQueryParams;
import it.cavallium.dbengine.lucene.searcher.LocalSearcher;
import it.cavallium.dbengine.rpc.current.data.IndicizerAnalyzers;
import it.cavallium.dbengine.rpc.current.data.IndicizerSimilarities;
import it.cavallium.dbengine.rpc.current.data.LuceneOptions;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.MergeScheduler;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.SimpleMergedSegmentWarmer;
import org.apache.lucene.index.SnapshotDeletionPolicy;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.InfoStream;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.warp.commonutils.type.ShortNamedThreadFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class LLLocalLuceneIndex implements LLLuceneIndex {

	protected static final Logger logger = LogManager.getLogger(LLLocalLuceneIndex.class);

	private final ReentrantLock shutdownLock = new ReentrantLock();
	/**
	 * Global lucene index scheduler.
	 * There is only a single thread globally to not overwhelm the disk with
	 * concurrent commits or concurrent refreshes.
	 */
	private static final Scheduler luceneHeavyTasksScheduler = uninterruptibleScheduler(Schedulers.newBoundedElastic(
			DEFAULT_BOUNDED_ELASTIC_SIZE,
			DEFAULT_BOUNDED_ELASTIC_QUEUESIZE,
			new ShortNamedThreadFactory("heavy-tasks").setDaemon(true).withGroup(new ThreadGroup("lucene-heavy-tasks")),
			Math.toIntExact(Duration.ofHours(1).toSeconds())
	));
	private static final Scheduler luceneWriteScheduler = uninterruptibleScheduler(Schedulers.newBoundedElastic(
			DEFAULT_BOUNDED_ELASTIC_SIZE,
			DEFAULT_BOUNDED_ELASTIC_QUEUESIZE,
			new ShortNamedThreadFactory("lucene-write").setDaemon(true).withGroup(new ThreadGroup("lucene-write")),
			Math.toIntExact(Duration.ofHours(1).toSeconds())
	));
	private static final Scheduler bulkScheduler = luceneWriteScheduler;

	static {
		LLUtils.initHooks();
	}

	private final LocalSearcher localSearcher;
	private final DecimalBucketMultiSearcher decimalBucketMultiSearcher = new DecimalBucketMultiSearcher();

	private final Counter startedDocIndexings;
	private final Counter endeddDocIndexings;
	private final Timer docIndexingTime;
	private final Timer snapshotTime;
	private final Timer flushTime;
	private final Timer commitTime;
	private final Timer mergeTime;
	private final Timer refreshTime;

	private final String shardName;
	private final IndexWriter indexWriter;
	private final SnapshotsManager snapshotsManager;
	private final IndexSearcherManager searcherManager;
	private final PerFieldAnalyzerWrapper luceneAnalyzer;
	private final Similarity luceneSimilarity;
	private final LuceneRocksDBManager rocksDBManager;
	private final Directory directory;
	private final boolean lowMemory;

	private final Phaser activeTasks = new Phaser(1);
	private final AtomicBoolean closeRequested = new AtomicBoolean();

	public LLLocalLuceneIndex(LLTempHugePqEnv env,
			MeterRegistry meterRegistry,
			@NotNull String clusterName,
			int shardIndex,
			IndicizerAnalyzers indicizerAnalyzers,
			IndicizerSimilarities indicizerSimilarities,
			LuceneOptions luceneOptions,
			@Nullable LuceneHacks luceneHacks,
			@Nullable LuceneRocksDBManager rocksDBManager) throws IOException {

		if (clusterName.isBlank()) {
			throw new IOException("Empty lucene database name");
		}
		if (!MMapDirectory.UNMAP_SUPPORTED) {
			logger.error("Unmap is unsupported, lucene will run slower: {}", MMapDirectory.UNMAP_NOT_SUPPORTED_REASON);
		} else {
			logger.debug("Lucene MMap is supported");
		}
		this.lowMemory = luceneOptions.lowMemory();
		this.shardName = LuceneUtils.getStandardName(clusterName, shardIndex);
		this.directory = LuceneUtils.createLuceneDirectory(luceneOptions.directoryOptions(),
				shardName,
				rocksDBManager);
		boolean isFilesystemCompressed = LuceneUtils.getIsFilesystemCompressed(luceneOptions.directoryOptions());

		var snapshotter = new SnapshotDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy());
		this.luceneAnalyzer = LuceneUtils.toPerFieldAnalyzerWrapper(indicizerAnalyzers);
		this.luceneSimilarity = LuceneUtils.toPerFieldSimilarityWrapper(indicizerSimilarities);
		this.rocksDBManager = rocksDBManager;

		var useHugePq = luceneOptions.allowNonVolatileCollection();
		var maxInMemoryResultEntries = luceneOptions.maxInMemoryResultEntries();
		if (luceneHacks != null && luceneHacks.customLocalSearcher() != null) {
			localSearcher = luceneHacks.customLocalSearcher().get();
		} else {
			localSearcher = new AdaptiveLocalSearcher(env, useHugePq, maxInMemoryResultEntries);
		}

		var indexWriterConfig = new IndexWriterConfig(luceneAnalyzer);
		indexWriterConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
		indexWriterConfig.setIndexDeletionPolicy(snapshotter);
		indexWriterConfig.setCommitOnClose(true);
		var mergePolicy = new TieredMergePolicy();
		indexWriterConfig.setMergePolicy(mergePolicy);
		indexWriterConfig.setMergedSegmentWarmer(new SimpleMergedSegmentWarmer(InfoStream.getDefault()));
		int writerSchedulerMaxThreadCount;
		MergeScheduler mergeScheduler;
		if (lowMemory) {
			mergeScheduler = new SerialMergeScheduler();
			writerSchedulerMaxThreadCount = 1;
		} else {
			var concurrentMergeScheduler = new ConcurrentMergeScheduler();
			// false means SSD, true means HDD
			concurrentMergeScheduler.setDefaultMaxMergesAndThreads(false);
			if (LuceneUtils.getManagedPath(luceneOptions.directoryOptions()).isEmpty()) {
				concurrentMergeScheduler.disableAutoIOThrottle();
			} else {
				concurrentMergeScheduler.enableAutoIOThrottle();
			}
			writerSchedulerMaxThreadCount = concurrentMergeScheduler.getMaxThreadCount();
			mergeScheduler = concurrentMergeScheduler;
		}
		if (isFilesystemCompressed) {
			indexWriterConfig.setUseCompoundFile(false);
			indexWriterConfig.setCodec(new Lucene91CodecWithNoFieldCompression());
		}
		logger.trace("WriterSchedulerMaxThreadCount: {}", writerSchedulerMaxThreadCount);
		indexWriterConfig.setMergeScheduler(mergeScheduler);
		if (luceneOptions.indexWriterRAMBufferSizeMB().isPresent()) {
			indexWriterConfig.setRAMBufferSizeMB(luceneOptions.indexWriterRAMBufferSizeMB().get());
		}
		if (luceneOptions.indexWriterMaxBufferedDocs().isPresent()) {
			indexWriterConfig.setMaxBufferedDocs(luceneOptions.indexWriterMaxBufferedDocs().get());
		}
		if (luceneOptions.indexWriterReaderPooling().isPresent()) {
			indexWriterConfig.setReaderPooling(luceneOptions.indexWriterReaderPooling().get());
		}
		indexWriterConfig.setSimilarity(getLuceneSimilarity());
		this.indexWriter = new IndexWriter(directory, indexWriterConfig);
		this.snapshotsManager = new SnapshotsManager(indexWriter, snapshotter);
		this.searcherManager = new CachedIndexSearcherManager(indexWriter,
				snapshotsManager,
				luceneHeavyTasksScheduler,
				getLuceneSimilarity(),
				luceneOptions.applyAllDeletes().orElse(true),
				luceneOptions.writeAllDeletes().orElse(false),
				luceneOptions.queryRefreshDebounceTime()
		);

		this.startedDocIndexings = meterRegistry.counter("index.write.doc.started.counter", "index.name", clusterName);
		this.endeddDocIndexings = meterRegistry.counter("index.write.doc.ended.counter", "index.name", clusterName);
		this.docIndexingTime = Timer.builder("index.write.doc.timer").publishPercentiles(0.2, 0.5, 0.95).publishPercentileHistogram().tag("index.name", clusterName).register(meterRegistry);
		this.snapshotTime = Timer.builder("index.write.snapshot.timer").publishPercentiles(0.2, 0.5, 0.95).publishPercentileHistogram().tag("index.name", clusterName).register(meterRegistry);
		this.flushTime = Timer.builder("index.write.flush.timer").publishPercentiles(0.2, 0.5, 0.95).publishPercentileHistogram().tag("index.name", clusterName).register(meterRegistry);
		this.commitTime = Timer.builder("index.write.commit.timer").publishPercentiles(0.2, 0.5, 0.95).publishPercentileHistogram().tag("index.name", clusterName).register(meterRegistry);
		this.mergeTime = Timer.builder("index.write.merge.timer").publishPercentiles(0.2, 0.5, 0.95).publishPercentileHistogram().tag("index.name", clusterName).register(meterRegistry);
		this.refreshTime = Timer.builder("index.search.refresh.timer").publishPercentiles(0.2, 0.5, 0.95).publishPercentileHistogram().tag("index.name", clusterName).register(meterRegistry);

		// Start scheduled tasks
		var commitMillis = luceneOptions.commitDebounceTime().toMillis();
		luceneHeavyTasksScheduler.schedulePeriodically(this::scheduledCommit, commitMillis, commitMillis,
				TimeUnit.MILLISECONDS);
	}

	private Similarity getLuceneSimilarity() {
		return luceneSimilarity;
	}

	@Override
	public String getLuceneIndexName() {
		return shardName;
	}

	@Override
	public Mono<LLSnapshot> takeSnapshot() {
		return snapshotsManager.takeSnapshot().elapsed().map(elapsed -> {
			snapshotTime.record(elapsed.getT1(), TimeUnit.MILLISECONDS);
			return elapsed.getT2();
		}).transform(this::ensureOpen);
	}

	private <V> Mono<V> ensureOpen(Mono<V> mono) {
		return Mono.<Void>fromCallable(() -> {
			if (closeRequested.get()) {
				throw new IllegalStateException("Lucene index is closed");
			} else {
				return null;
			}
		}).then(mono).doFirst(activeTasks::register).doFinally(s -> activeTasks.arriveAndDeregister());
	}

	private <V> Mono<V> runSafe(Callable<V> callable) {
		return Mono
				.fromCallable(callable)
				.subscribeOn(luceneWriteScheduler)
				.publishOn(Schedulers.parallel());
	}

	@Override
	public Mono<Void> releaseSnapshot(LLSnapshot snapshot) {
		return snapshotsManager
				.releaseSnapshot(snapshot)
				.elapsed()
				.doOnNext(elapsed -> snapshotTime.record(elapsed.getT1(), TimeUnit.MILLISECONDS))
				.then();
	}

	@Override
	public Mono<Void> addDocument(LLTerm key, LLUpdateDocument doc) {
		return this.<Void>runSafe(() -> {
			docIndexingTime.recordCallable(() -> {
				startedDocIndexings.increment();
				try {
					indexWriter.addDocument(toDocument(doc));
				} finally {
					endeddDocIndexings.increment();
				}
				return null;
			});
			logger.trace(MARKER_LUCENE, "Added document {}: {}", key, doc);
			return null;
		}).transform(this::ensureOpen);
	}

	@Override
	public Mono<Long> addDocuments(boolean atomic, Flux<Entry<LLTerm, LLUpdateDocument>> documents) {
		if (!atomic) {
			return documents
					.publishOn(bulkScheduler)
					.handle((document, sink) -> {
						LLUpdateDocument value = document.getValue();
						startedDocIndexings.increment();
						try {
							docIndexingTime.recordCallable(() -> {
								indexWriter.addDocument(toDocument(value));
								return null;
							});
						} catch (Exception ex) {
							sink.error(ex);
							return;
						} finally {
							endeddDocIndexings.increment();
						}
						logger.trace(MARKER_LUCENE, "Added document: {}", document);
						sink.next(true);
					})
					.count()
					.transform(this::ensureOpen);
		} else {
			return documents
					.collectList()
					.publishOn(bulkScheduler)
					.<Long>handle((documentsList, sink) -> {
						var count = documentsList.size();
						StopWatch stopWatch = StopWatch.createStarted();
						try {
							startedDocIndexings.increment(count);
							try {
								indexWriter.addDocuments(LLUtils.toDocumentsFromEntries(documentsList));
							} finally {
								endeddDocIndexings.increment(count);
							}
						} catch (IOException ex) {
							sink.error(ex);
							return;
						} finally {
							docIndexingTime.record(stopWatch.getTime(TimeUnit.MILLISECONDS) / Math.max(count, 1),
									TimeUnit.MILLISECONDS
							);
						}
						sink.next((long) documentsList.size());
					})
					.transform(this::ensureOpen);
		}
	}


	@Override
	public Mono<Void> deleteDocument(LLTerm id) {
		return this.<Void>runSafe(() -> docIndexingTime.recordCallable(() -> {
			startedDocIndexings.increment();
			try {
				indexWriter.deleteDocuments(LLUtils.toTerm(id));
			} finally {
				endeddDocIndexings.increment();
			}
			return null;
		})).transform(this::ensureOpen);
	}

	@Override
	public Mono<Void> update(LLTerm id, LLIndexRequest request) {
		return this.<Void>runSafe(() -> {
			docIndexingTime.recordCallable(() -> {
				startedDocIndexings.increment();
				try {
					if (request instanceof LLUpdateDocument updateDocument) {
						indexWriter.updateDocument(LLUtils.toTerm(id), toDocument(updateDocument));
					} else if (request instanceof LLSoftUpdateDocument softUpdateDocument) {
						indexWriter.softUpdateDocument(LLUtils.toTerm(id),
								toDocument(softUpdateDocument.items()),
								toFields(softUpdateDocument.softDeleteItems())
						);
					} else if (request instanceof LLUpdateFields updateFields) {
						indexWriter.updateDocValues(LLUtils.toTerm(id), toFields(updateFields.items()));
					} else {
						throw new UnsupportedOperationException("Unexpected request type: " + request);
					}
				} finally {
					endeddDocIndexings.increment();
				}
				return null;
			});
			logger.trace(MARKER_LUCENE, "Updated document {}: {}", id, request);
			return null;
		}).transform(this::ensureOpen);
	}

	@Override
	public Mono<Long> updateDocuments(Flux<Entry<LLTerm, LLUpdateDocument>> documents) {
		return documents
				.log("local-update-documents", Level.FINEST, false, SignalType.ON_NEXT, SignalType.ON_COMPLETE)
				.publishOn(bulkScheduler)
				.handle((document, sink) -> {
					LLTerm key = document.getKey();
					LLUpdateDocument value = document.getValue();
					startedDocIndexings.increment();
					try {
						docIndexingTime.recordCallable(() -> {
							indexWriter.updateDocument(LLUtils.toTerm(key), toDocument(value));
							return null;
						});
						logger.trace(MARKER_LUCENE, "Updated document {}: {}", key, value);
					} catch (Exception ex) {
						sink.error(ex);
						return;
					} finally {
						endeddDocIndexings.increment();
					}
					sink.next(true);
				})
				.count()
				.transform(this::ensureOpen);
	}


	@Override
	public Mono<Void> deleteAll() {
		return this.<Void>runSafe(() -> {
			shutdownLock.lock();
			try {
				indexWriter.deleteAll();
				indexWriter.forceMergeDeletes(true);
				indexWriter.commit();
			} finally {
				shutdownLock.unlock();
			}
			return null;
		}).subscribeOn(luceneHeavyTasksScheduler).publishOn(Schedulers.parallel()).transform(this::ensureOpen);
	}

	@Override
	public Flux<LLSearchResultShard> moreLikeThis(@Nullable LLSnapshot snapshot,
			QueryParams queryParams,
			@Nullable String keyFieldName,
			Multimap<String, String> mltDocumentFieldsFlux) {
		LocalQueryParams localQueryParams = LuceneUtils.toLocalQueryParams(queryParams, luceneAnalyzer);
		var searcher = this.searcherManager.retrieveSearcher(snapshot);
		var transformer = new MoreLikeThisTransformer(mltDocumentFieldsFlux, luceneAnalyzer, luceneSimilarity);

		return localSearcher
				.collect(searcher, localQueryParams, keyFieldName, transformer)
				.map(result -> new LLSearchResultShard(result.results(), result.totalHitsCount(), result::close))
				.flux();
	}

	@Override
	public Flux<LLSearchResultShard> search(@Nullable LLSnapshot snapshot, QueryParams queryParams,
			@Nullable String keyFieldName) {
		LocalQueryParams localQueryParams = LuceneUtils.toLocalQueryParams(queryParams, luceneAnalyzer);
		var searcher = searcherManager.retrieveSearcher(snapshot);

		return localSearcher
				.collect(searcher, localQueryParams, keyFieldName, NO_REWRITE)
				.map(result -> new LLSearchResultShard(result.results(), result.totalHitsCount(), result::close))
				.flux();
	}

	@Override
	public Mono<Buckets> computeBuckets(@Nullable LLSnapshot snapshot,
			@NotNull List<Query> queries,
			@Nullable Query normalizationQuery,
			BucketParams bucketParams) {
		List<org.apache.lucene.search.Query> localQueries = new ArrayList<>(queries.size());
		for (Query query : queries) {
			localQueries.add(QueryParser.toQuery(query, luceneAnalyzer));
		}
		var localNormalizationQuery = QueryParser.toQuery(normalizationQuery, luceneAnalyzer);
		var searchers = searcherManager
				.retrieveSearcher(snapshot)
				.map(indexSearcher -> LLIndexSearchers.unsharded(indexSearcher).send());

		return decimalBucketMultiSearcher.collectMulti(searchers, bucketParams, localQueries, localNormalizationQuery);
	}

	public Mono<Send<LLIndexSearcher>> retrieveSearcher(@Nullable LLSnapshot snapshot) {
		return searcherManager.retrieveSearcher(snapshot);
	}

	@Override
	public Mono<Void> close() {
		return Mono
				.<Void>fromCallable(() -> {
					logger.info("Waiting IndexWriter tasks...");
					activeTasks.arriveAndAwaitAdvance();
					logger.info("IndexWriter tasks ended");
					return null;
				})
				.subscribeOn(luceneHeavyTasksScheduler)
				.then(searcherManager.close())
				.then(Mono.<Void>fromCallable(() -> {
					shutdownLock.lock();
					try {
						logger.info("Closing IndexWriter...");
						indexWriter.close();
						directory.close();
						logger.info("IndexWriter closed");
					} finally {
						shutdownLock.unlock();
					}
					return null;
				}).subscribeOn(luceneHeavyTasksScheduler))

				// Avoid closing multiple times
				.transformDeferred(mono -> {
					if (this.closeRequested.compareAndSet(false, true)) {
						logger.trace("Set closeRequested to true. Further update/write calls will result in an error");
						return mono;
					} else {
						logger.debug("Tried to close more than once");
						return Mono.empty();
					}
				});
	}

	@Override
	public Mono<Void> flush() {
		return Mono
				.<Void>fromCallable(() -> {
					if (activeTasks.isTerminated()) return null;
					shutdownLock.lock();
					try {
						if (closeRequested.get()) {
							return null;
						}
						flushTime.recordCallable(() -> {
							indexWriter.flush();
							return null;
						});
					} finally {
						shutdownLock.unlock();
					}
					return null;
				})
				.subscribeOn(luceneHeavyTasksScheduler)
				.transform(this::ensureOpen);
	}

	@Override
	public Mono<Void> refresh(boolean force) {
		return Mono
				.<Void>fromCallable(() -> {
					activeTasks.register();
					try {
						if (activeTasks.isTerminated()) return null;
						shutdownLock.lock();
						try {
							if (closeRequested.get()) {
								return null;
							}
							refreshTime.recordCallable(() -> {
								if (force) {
									searcherManager.maybeRefreshBlocking();
								} else {
									searcherManager.maybeRefresh();
								}
								return null;
							});
						} finally {
							shutdownLock.unlock();
						}
					} finally {
						activeTasks.arriveAndDeregister();
					}
					return null;
				})
				.subscribeOn(luceneHeavyTasksScheduler);
	}

	/**
	 * Internal method, do not use
	 */
	public void scheduledCommit() {
		shutdownLock.lock();
		try {
			if (closeRequested.get()) {
				return;
			}
			commitTime.recordCallable(() -> {
				indexWriter.commit();
				return null;
			});
		} catch (Exception ex) {
			logger.error(MARKER_LUCENE, "Failed to execute a scheduled commit", ex);
		} finally {
			shutdownLock.unlock();
		}
	}

	/**
	 * Internal method, do not use
	 */
	public void scheduledMerge() { // Do not use. Merges are done automatically by merge policies
		shutdownLock.lock();
		try {
			if (closeRequested.get()) {
				return;
			}
			mergeTime.recordCallable(() -> {
				indexWriter.maybeMerge();
				return null;
			});
		} catch (Exception ex) {
			logger.error(MARKER_LUCENE, "Failed to execute a scheduled merge", ex);
		} finally {
			shutdownLock.unlock();
		}
	}

	@Override
	public boolean isLowMemoryMode() {
		return lowMemory;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		LLLocalLuceneIndex that = (LLLocalLuceneIndex) o;

		return Objects.equals(shardName, that.shardName);
	}

	@Override
	public int hashCode() {
		return shardName.hashCode();
	}
}
