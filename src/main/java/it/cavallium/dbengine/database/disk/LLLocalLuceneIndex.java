package it.cavallium.dbengine.database.disk;

import static it.cavallium.dbengine.database.LLUtils.MARKER_LUCENE;
import static it.cavallium.dbengine.database.LLUtils.toDocument;
import static it.cavallium.dbengine.database.LLUtils.toFields;
import static it.cavallium.dbengine.lucene.searcher.GlobalQueryRewrite.NO_REWRITE;
import static java.util.Objects.requireNonNull;

import com.google.common.collect.Multimap;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import it.cavallium.dbengine.client.Backuppable;
import it.cavallium.dbengine.client.IBackuppable;
import it.cavallium.dbengine.client.query.QueryParser;
import it.cavallium.dbengine.client.query.current.data.Query;
import it.cavallium.dbengine.client.query.current.data.QueryParams;
import it.cavallium.dbengine.client.query.current.data.TotalHitsCount;
import it.cavallium.dbengine.database.LLIndexRequest;
import it.cavallium.dbengine.database.LLLuceneIndex;
import it.cavallium.dbengine.database.LLSearchResultShard;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.LLSoftUpdateDocument;
import it.cavallium.dbengine.database.LLTerm;
import it.cavallium.dbengine.database.LLUpdateDocument;
import it.cavallium.dbengine.database.LLUpdateFields;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.lucene.LuceneCloseable;
import it.cavallium.dbengine.lucene.LuceneConcurrentMergeScheduler;
import it.cavallium.dbengine.lucene.LuceneHacks;
import it.cavallium.dbengine.lucene.LuceneUtils;
import it.cavallium.dbengine.lucene.collector.Buckets;
import it.cavallium.dbengine.lucene.directory.Lucene91CodecWithNoFieldCompression;
import it.cavallium.dbengine.lucene.mlt.MoreLikeThisTransformer;
import it.cavallium.dbengine.lucene.searcher.AdaptiveLocalSearcher;
import it.cavallium.dbengine.lucene.searcher.BucketParams;
import it.cavallium.dbengine.lucene.searcher.DecimalBucketMultiSearcher;
import it.cavallium.dbengine.lucene.searcher.LocalQueryParams;
import it.cavallium.dbengine.lucene.searcher.LocalSearcher;
import it.cavallium.dbengine.lucene.searcher.LuceneSearchResult;
import it.cavallium.dbengine.rpc.current.data.IndicizerAnalyzers;
import it.cavallium.dbengine.rpc.current.data.IndicizerSimilarities;
import it.cavallium.dbengine.rpc.current.data.LuceneOptions;
import it.cavallium.dbengine.utils.SimpleResource;
import java.io.IOException;
import it.cavallium.dbengine.utils.DBException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.stream.Stream;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MergeScheduler;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.SnapshotDeletionPolicy;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.IOSupplier;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class LLLocalLuceneIndex extends SimpleResource implements IBackuppable, LLLuceneIndex, LuceneCloseable {

	protected static final Logger logger = LogManager.getLogger(LLLocalLuceneIndex.class);

	private final ReentrantLock shutdownLock = new ReentrantLock();
	/**
	 * Global lucene index scheduler.
	 * There is only a single thread globally to not overwhelm the disk with
	 * concurrent commits or concurrent refreshes.
	 */
	private static final ScheduledExecutorService luceneHeavyTasksScheduler = Executors.newScheduledThreadPool(4,
			new LuceneThreadFactory("heavy-tasks").setDaemon(true).withGroup(new ThreadGroup("lucene-heavy-tasks"))
	);
	private static final ScheduledExecutorService luceneWriteScheduler = Executors.newScheduledThreadPool(8,
			new LuceneThreadFactory("lucene-write").setDaemon(true).withGroup(new ThreadGroup("lucene-write"))
	);
	private static final ScheduledExecutorService bulkScheduler = luceneWriteScheduler;

	private static final boolean ENABLE_SNAPSHOTS
			= Boolean.parseBoolean(System.getProperty("it.cavallium.dbengine.lucene.snapshot.enable", "true"));

	private static final boolean CACHE_SEARCHER_MANAGER
			= Boolean.parseBoolean(System.getProperty("it.cavallium.dbengine.lucene.cachedsearchermanager.enable", "true"));

	private static final LLSnapshot DUMMY_SNAPSHOT = new LLSnapshot(-1);

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
	private final Directory directory;
	private final LuceneBackuppable backuppable;
	private final boolean lowMemory;

	private final Phaser activeTasks = new Phaser(1);

	public LLLocalLuceneIndex(MeterRegistry meterRegistry,
			@NotNull String clusterName,
			int shardIndex,
			IndicizerAnalyzers indicizerAnalyzers,
			IndicizerSimilarities indicizerSimilarities,
			LuceneOptions luceneOptions,
			@Nullable LuceneHacks luceneHacks) {

		if (clusterName.isBlank()) {
			throw new DBException("Empty lucene database name");
		}
		if (!MMapDirectory.UNMAP_SUPPORTED) {
			logger.error("Unmap is unsupported, lucene will run slower: {}", MMapDirectory.UNMAP_NOT_SUPPORTED_REASON);
		} else {
			logger.debug("Lucene MMap is supported");
		}
		this.lowMemory = luceneOptions.lowMemory();
		this.shardName = LuceneUtils.getStandardName(clusterName, shardIndex);
		try {
			this.directory = LuceneUtils.createLuceneDirectory(luceneOptions.directoryOptions(), shardName);
		} catch (IOException e) {
			throw new DBException(e);
		}
		boolean isFilesystemCompressed = LuceneUtils.getIsFilesystemCompressed(luceneOptions.directoryOptions());

		this.luceneAnalyzer = LuceneUtils.toPerFieldAnalyzerWrapper(indicizerAnalyzers);
		this.luceneSimilarity = LuceneUtils.toPerFieldSimilarityWrapper(indicizerSimilarities);

		var maxInMemoryResultEntries = luceneOptions.maxInMemoryResultEntries();
		if (luceneHacks != null && luceneHacks.customLocalSearcher() != null) {
			localSearcher = luceneHacks.customLocalSearcher().get();
		} else {
			localSearcher = new AdaptiveLocalSearcher(maxInMemoryResultEntries);
		}

		var indexWriterConfig = new IndexWriterConfig(luceneAnalyzer);
		IndexDeletionPolicy deletionPolicy;
		deletionPolicy = requireNonNull(indexWriterConfig.getIndexDeletionPolicy());
		if (ENABLE_SNAPSHOTS) {
			deletionPolicy = new SnapshotDeletionPolicy(deletionPolicy);
		}
		indexWriterConfig.setIndexDeletionPolicy(deletionPolicy);
		indexWriterConfig.setCommitOnClose(true);
		int writerSchedulerMaxThreadCount;
		MergeScheduler mergeScheduler;
		if (lowMemory) {
			mergeScheduler = new SerialMergeScheduler();
			writerSchedulerMaxThreadCount = 1;
		} else {
			//noinspection resource
			ConcurrentMergeScheduler concurrentMergeScheduler = new LuceneConcurrentMergeScheduler();
			// false means SSD, true means HDD
			boolean spins = false;
			concurrentMergeScheduler.setDefaultMaxMergesAndThreads(spins);
			// It's true by default, but this makes sure it's true if it's a managed path
			if (LuceneUtils.getManagedPath(luceneOptions.directoryOptions()).isPresent()) {
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
		indexWriterConfig.setMergePolicy(LuceneUtils.getMergePolicy(luceneOptions));
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
		try {
			this.indexWriter = new IndexWriter(directory, indexWriterConfig);
		} catch (IOException e) {
			throw new DBException(e);
		}
		if (ENABLE_SNAPSHOTS) {
			this.snapshotsManager = new SnapshotsManager(indexWriter, (SnapshotDeletionPolicy) deletionPolicy);
		} else {
			this.snapshotsManager = null;
		}
		SimpleIndexSearcherManager searcherManager;
		if (CACHE_SEARCHER_MANAGER) {
			searcherManager = new SimpleIndexSearcherManager(indexWriter,
					snapshotsManager,
					luceneHeavyTasksScheduler,
					getLuceneSimilarity(),
					luceneOptions.applyAllDeletes().orElse(true),
					luceneOptions.writeAllDeletes().orElse(false),
					luceneOptions.queryRefreshDebounceTime()
			);
		} else {
			searcherManager = new SimpleIndexSearcherManager(indexWriter,
					snapshotsManager,
					luceneHeavyTasksScheduler,
					getLuceneSimilarity(),
					luceneOptions.applyAllDeletes().orElse(true),
					luceneOptions.writeAllDeletes().orElse(false),
					luceneOptions.queryRefreshDebounceTime());
		}
		this.searcherManager = searcherManager;

		this.startedDocIndexings = meterRegistry.counter("index.write.doc.started.counter", "index.name", clusterName);
		this.endeddDocIndexings = meterRegistry.counter("index.write.doc.ended.counter", "index.name", clusterName);
		this.docIndexingTime = Timer.builder("index.write.doc.timer").publishPercentiles(0.2, 0.5, 0.95).publishPercentileHistogram().tag("index.name", clusterName).register(meterRegistry);
		this.snapshotTime = Timer.builder("index.write.snapshot.timer").publishPercentiles(0.2, 0.5, 0.95).publishPercentileHistogram().tag("index.name", clusterName).register(meterRegistry);
		this.flushTime = Timer.builder("index.write.flush.timer").publishPercentiles(0.2, 0.5, 0.95).publishPercentileHistogram().tag("index.name", clusterName).register(meterRegistry);
		this.commitTime = Timer.builder("index.write.commit.timer").publishPercentiles(0.2, 0.5, 0.95).publishPercentileHistogram().tag("index.name", clusterName).register(meterRegistry);
		this.mergeTime = Timer.builder("index.write.merge.timer").publishPercentiles(0.2, 0.5, 0.95).publishPercentileHistogram().tag("index.name", clusterName).register(meterRegistry);
		this.refreshTime = Timer.builder("index.search.refresh.timer").publishPercentiles(0.2, 0.5, 0.95).publishPercentileHistogram().tag("index.name", clusterName).register(meterRegistry);
		meterRegistry.gauge("index.snapshot.counter", List.of(Tag.of("index.name", clusterName)), this, LLLocalLuceneIndex::getSnapshotsCount);
		meterRegistry.gauge("index.write.flushing.bytes", List.of(Tag.of("index.name", clusterName)), this, LLLocalLuceneIndex::getIndexWriterFlushingBytes);
		meterRegistry.gauge("index.write.sequence.completed.max", List.of(Tag.of("index.name", clusterName)), this, LLLocalLuceneIndex::getIndexWriterMaxCompletedSequenceNumber);
		meterRegistry.gauge("index.write.doc.pending.counter", List.of(Tag.of("index.name", clusterName)), this, LLLocalLuceneIndex::getIndexWriterPendingNumDocs);
		meterRegistry.gauge("index.write.segment.merging.counter", List.of(Tag.of("index.name", clusterName)), this, LLLocalLuceneIndex::getIndexWriterMergingSegmentsSize);
		meterRegistry.gauge("index.directory.deletion.pending.counter", List.of(Tag.of("index.name", clusterName)), this, LLLocalLuceneIndex::getDirectoryPendingDeletionsCount);
		meterRegistry.gauge("index.doc.counter", List.of(Tag.of("index.name", clusterName)), this, LLLocalLuceneIndex::getDocCount);
		meterRegistry.gauge("index.doc.max", List.of(Tag.of("index.name", clusterName)), this, LLLocalLuceneIndex::getMaxDoc);
		meterRegistry.gauge("index.searcher.refreshes.active.count",
				List.of(Tag.of("index.name", clusterName)),
				searcherManager,
				SimpleIndexSearcherManager::getActiveRefreshes
		);
		meterRegistry.gauge("index.searcher.searchers.active.count",
				List.of(Tag.of("index.name", clusterName)),
				searcherManager,
				SimpleIndexSearcherManager::getActiveSearchers
		);

		// Start scheduled tasks
		var commitMillis = luceneOptions.commitDebounceTime().toMillis();
		luceneHeavyTasksScheduler.scheduleAtFixedRate(this::scheduledCommit, commitMillis, commitMillis,
				TimeUnit.MILLISECONDS);

		this.backuppable = new LuceneBackuppable();
	}

	private Similarity getLuceneSimilarity() {
		return luceneSimilarity;
	}

	@Override
	public String getLuceneIndexName() {
		return shardName;
	}

	@Override
	public LLSnapshot takeSnapshot() {
		return runTask(() -> {
			if (snapshotsManager == null) {
				return DUMMY_SNAPSHOT;
			}
			try {
				return snapshotTime.recordCallable(snapshotsManager::takeSnapshot);
			} catch (Exception e) {
				throw new DBException("Failed to take snapshot", e);
			}
		});
	}

	private <V> V runTask(Supplier<V> supplier) {
		if (isClosed()) {
			throw new IllegalStateException("Lucene index is closed");
		} else {
			activeTasks.register();
			try {
				return supplier.get();
			} finally {
				activeTasks.arriveAndDeregister();
			}
		}
	}

	@Override
	public void releaseSnapshot(LLSnapshot snapshot) {
		if (snapshotsManager == null) {
			if (snapshot != null && !Objects.equals(snapshot, DUMMY_SNAPSHOT)) {
				throw new IllegalStateException("Can't release snapshot " + snapshot);
			}
			return;
		}
		snapshotsManager.releaseSnapshot(snapshot);
	}

	@Override
	public void addDocument(LLTerm key, LLUpdateDocument doc) {
		runTask(() -> {
			try {
				docIndexingTime.recordCallable(() -> {
					startedDocIndexings.increment();
					try {
						indexWriter.addDocument(toDocument(doc));
					} finally {
						endeddDocIndexings.increment();
					}
					return null;
				});
			} catch (Exception e) {
				throw new DBException("Failed to add document", e);
			}
			logger.trace(MARKER_LUCENE, "Added document {}: {}", key, doc);
			return null;
		});
	}

	@Override
	public long addDocuments(boolean atomic, Stream<Entry<LLTerm, LLUpdateDocument>> documents) {
		return this.runTask(() -> {
			if (!atomic) {
				LongAdder count = new LongAdder();
				documents.forEach(document -> {
					count.increment();
					LLUpdateDocument value = document.getValue();
					startedDocIndexings.increment();
					try {
						docIndexingTime.recordCallable(() -> {
							indexWriter.addDocument(toDocument(value));
							return null;
						});
					} catch (Exception ex) {
						throw new CompletionException("Failed to add document", ex);
					} finally {
						endeddDocIndexings.increment();
					}
					logger.trace(MARKER_LUCENE, "Added document: {}", document);
				});
				return count.sum();
			} else {
				var documentsList = documents.toList();
				var count = documentsList.size();
				StopWatch stopWatch = StopWatch.createStarted();
				try {
					startedDocIndexings.increment(count);
					try {
						indexWriter.addDocuments(LLUtils.toDocumentsFromEntries(documentsList));
					} catch (IOException e) {
						throw new DBException(e);
					} finally {
						endeddDocIndexings.increment(count);
					}
				} finally {
					docIndexingTime.record(stopWatch.getTime(TimeUnit.MILLISECONDS) / Math.max(count, 1),
							TimeUnit.MILLISECONDS
					);
				}
				return (long) documentsList.size();
			}
		});
	}


	@Override
	public void deleteDocument(LLTerm id) {
		this.runTask(() -> {
			try {
				return docIndexingTime.recordCallable(() -> {
					startedDocIndexings.increment();
					try {
						indexWriter.deleteDocuments(LLUtils.toTerm(id));
					} finally {
						endeddDocIndexings.increment();
					}
					return null;
				});
			} catch (Exception e) {
				throw new DBException("Failed to delete document", e);
			}
		});
	}

	@Override
	public void update(LLTerm id, LLIndexRequest request) {
		this.runTask(() -> {
			try {
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
			} catch (Exception e) {
				throw new DBException("Failed to update document", e);
			}
			logger.trace(MARKER_LUCENE, "Updated document {}: {}", id, request);
			return null;
		});
	}

	@Override
	public long updateDocuments(Stream<Entry<LLTerm, LLUpdateDocument>> documents) {
		return runTask(() -> {
			var count = new LongAdder();
			documents.forEach(document -> {
				count.increment();
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
					throw new CompletionException(ex);
				} finally {
					endeddDocIndexings.increment();
				}
			});
			return count.sum();
		});
	}

	@Override
	public void deleteAll() {
		this.runTask(() -> {
			shutdownLock.lock();
			try {
				indexWriter.deleteAll();
				indexWriter.forceMergeDeletes(true);
				indexWriter.commit();
				indexWriter.deleteUnusedFiles();
			} catch (IOException e) {
				throw new DBException(e);
			} finally {
				shutdownLock.unlock();
			}
			return null;
		});
	}

	@Override
	public Stream<LLSearchResultShard> moreLikeThis(@Nullable LLSnapshot snapshot,
			QueryParams queryParams,
			@Nullable String keyFieldName,
			Multimap<String, String> mltDocumentFieldsFlux) {
		LocalQueryParams localQueryParams = LuceneUtils.toLocalQueryParams(queryParams, luceneAnalyzer);
		var searcher = this.searcherManager.retrieveSearcher(snapshot);
		var transformer = new MoreLikeThisTransformer(mltDocumentFieldsFlux, luceneAnalyzer, luceneSimilarity);

		var result = localSearcher.collect(searcher, localQueryParams, keyFieldName, transformer);
		return Stream.of(LLSearchResultShard.withResource(result.results(), result.totalHitsCount(), result));
	}

	@Override
	public Stream<LLSearchResultShard> search(@Nullable LLSnapshot snapshot, QueryParams queryParams,
			@Nullable String keyFieldName) {
		var result = searchInternal(snapshot, queryParams, keyFieldName);
		return Stream.of(LLSearchResultShard.withResource(result.results(), result.totalHitsCount(), result));
	}

	public LuceneSearchResult searchInternal(@Nullable LLSnapshot snapshot, QueryParams queryParams,
			@Nullable String keyFieldName) {
		LocalQueryParams localQueryParams = LuceneUtils.toLocalQueryParams(queryParams, luceneAnalyzer);
		var searcher = searcherManager.retrieveSearcher(snapshot);

		return localSearcher.collect(searcher, localQueryParams, keyFieldName, NO_REWRITE);
	}

	@Override
	public TotalHitsCount count(@Nullable LLSnapshot snapshot, Query query, @Nullable Duration timeout) {
		var params = LuceneUtils.getCountQueryParams(query);
		try (var result = this.searchInternal(snapshot, params, null)) {
			if (result == null) return TotalHitsCount.of(0, true);
			return result.totalHitsCount();
		}
	}

	@Override
	public Buckets computeBuckets(@Nullable LLSnapshot snapshot,
			@NotNull List<Query> queries,
			@Nullable Query normalizationQuery,
			BucketParams bucketParams) {
		List<org.apache.lucene.search.Query> localQueries = new ArrayList<>(queries.size());
		for (Query query : queries) {
			localQueries.add(QueryParser.toQuery(query, luceneAnalyzer));
		}
		var localNormalizationQuery = QueryParser.toQuery(normalizationQuery, luceneAnalyzer);
		LLIndexSearchers searchers = LLIndexSearchers.unsharded(searcherManager.retrieveSearcher(snapshot));

		return decimalBucketMultiSearcher.collectMulti(searchers, bucketParams, localQueries, localNormalizationQuery);
	}

	public LLIndexSearcher retrieveSearcher(@Nullable LLSnapshot snapshot) {
		return searcherManager.retrieveSearcher(snapshot);
	}

	@Override
	protected void onClose() {
		logger.debug("Waiting IndexWriter tasks...");
		activeTasks.arriveAndAwaitAdvance();
		logger.debug("IndexWriter tasks ended");
		shutdownLock.lock();
		try {
			logger.debug("Closing searcher manager...");
			searcherManager.close();
			logger.debug("Searcher manager closed");
			logger.debug("Closing IndexWriter...");
			indexWriter.close();
			directory.close();
			logger.debug("IndexWriter closed");
		} catch (IOException ex) {
			throw new DBException(ex);
		} finally {
			shutdownLock.unlock();
		}
	}

	@Override
	public void flush() {
		runTask(() -> {
			if (activeTasks.isTerminated()) return null;
			shutdownLock.lock();
			try {
				if (isClosed()) {
					return null;
				}
				flushTime.recordCallable(() -> {
					indexWriter.flush();
					return null;
				});
			} catch (Exception e) {
				throw new DBException("Failed to flush", e);
			} finally {
				shutdownLock.unlock();
			}
			return null;
		});
	}

	@Override
	public void waitForMerges() {
		runTask(() -> {
			if (activeTasks.isTerminated()) return null;
			shutdownLock.lock();
			try {
				if (isClosed()) {
					return null;
				}
				var mergeScheduler = indexWriter.getConfig().getMergeScheduler();
				if (mergeScheduler instanceof ConcurrentMergeScheduler concurrentMergeScheduler) {
					concurrentMergeScheduler.sync();
				}
			} finally {
				shutdownLock.unlock();
			}
			return null;
		});
	}

	@Override
	public void waitForLastMerges() {
		runTask(() -> {
			if (activeTasks.isTerminated()) return null;
			shutdownLock.lock();
			try {
				if (isClosed()) {
					return null;
				}
				indexWriter.getConfig().setMergePolicy(NoMergePolicy.INSTANCE);
				var mergeScheduler = indexWriter.getConfig().getMergeScheduler();
				if (mergeScheduler instanceof ConcurrentMergeScheduler concurrentMergeScheduler) {
					concurrentMergeScheduler.sync();
				}
				indexWriter.deleteUnusedFiles();
			} catch (IOException e) {
				throw new DBException(e);
			} finally {
				shutdownLock.unlock();
			}
			return null;
		});
	}

	@Override
	public void refresh(boolean force) {
		runTask(() -> {
			activeTasks.register();
			try {
				if (activeTasks.isTerminated()) return null;
				shutdownLock.lock();
				try {
					if (isClosed()) {
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
				} catch (Exception e) {
					throw new DBException("Failed to refresh", e);
				} finally {
					shutdownLock.unlock();
				}
			} finally {
				activeTasks.arriveAndDeregister();
			}
			return null;
		});
	}

	/**
	 * Internal method, do not use
	 */
	public void scheduledCommit() {
		shutdownLock.lock();
		try {
			if (isClosed()) {
				return;
			}
			commitTime.recordCallable(() -> {
				indexWriter.commit();
				indexWriter.deleteUnusedFiles();
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
			if (isClosed()) {
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

	private double getSnapshotsCount() {
		shutdownLock.lock();
		try {
			if (isClosed()) {
				return 0d;
			}
			if (snapshotsManager == null) return 0d;
			return snapshotsManager.getSnapshotsCount();
		} finally {
			shutdownLock.unlock();
		}
	}

	private double getIndexWriterFlushingBytes() {
		shutdownLock.lock();
		try {
			if (isClosed()) {
				return 0d;
			}
			return indexWriter.getFlushingBytes();
		} finally {
			shutdownLock.unlock();
		}
	}

	private double getIndexWriterMaxCompletedSequenceNumber() {
		shutdownLock.lock();
		try {
			if (isClosed()) {
				return 0d;
			}
			return indexWriter.getMaxCompletedSequenceNumber();
		} finally {
			shutdownLock.unlock();
		}
	}

	private double getIndexWriterPendingNumDocs() {
		shutdownLock.lock();
		try {
			if (isClosed()) {
				return 0d;
			}
			return indexWriter.getPendingNumDocs();
		} finally {
			shutdownLock.unlock();
		}
	}

	private double getIndexWriterMergingSegmentsSize() {
		shutdownLock.lock();
		try {
			if (isClosed()) {
				return 0d;
			}
			return indexWriter.getMergingSegments().size();
		} finally {
			shutdownLock.unlock();
		}
	}

	private double getDirectoryPendingDeletionsCount() {
		shutdownLock.lock();
		try {
			if (isClosed()) {
				return 0d;
			}
			return indexWriter.getDirectory().getPendingDeletions().size();
		} catch (IOException e) {
			return 0d;
		} finally {
			shutdownLock.unlock();
		}
	}

	private double getDocCount() {
		shutdownLock.lock();
		try {
			if (isClosed()) {
				return 0d;
			}
			var docStats = indexWriter.getDocStats();
			if (docStats != null) {
				return docStats.numDocs;
			} else {
				return 0d;
			}
		} finally {
			shutdownLock.unlock();
		}
	}

	private double getMaxDoc() {
		shutdownLock.lock();
		try {
			if (isClosed()) {
				return 0d;
			}
			var docStats = indexWriter.getDocStats();
			if (docStats != null) {
				return docStats.maxDoc;
			} else {
				return 0d;
			}
		} finally {
			shutdownLock.unlock();
		}
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

	@Override
	public void pauseForBackup() {
		backuppable.pauseForBackup();
	}

	@Override
	public void resumeAfterBackup() {
		backuppable.resumeAfterBackup();
	}

	@Override
	public boolean isPaused() {
		return backuppable.isPaused();
	}

	private class LuceneBackuppable extends Backuppable {

		private LLSnapshot snapshot;

		@Override
		protected void onPauseForBackup() {
			var snapshot = LLLocalLuceneIndex.this.takeSnapshot();
			if (snapshot == null) {
				logger.error("Can't pause index \"{}\" because snapshots are not enabled!", shardName);
			}
			this.snapshot = snapshot;
		}

		@Override
		protected void onResumeAfterBackup() {
			if (snapshot == null) {
				return;
			}
			LLLocalLuceneIndex.this.releaseSnapshot(snapshot);
		}
	}
}
