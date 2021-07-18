package it.cavallium.dbengine.database.disk;

import it.cavallium.dbengine.client.DirectIOOptions;
import it.cavallium.dbengine.client.IndicizerAnalyzers;
import it.cavallium.dbengine.client.IndicizerSimilarities;
import it.cavallium.dbengine.client.LuceneOptions;
import it.cavallium.dbengine.client.NRTCachingOptions;
import it.cavallium.dbengine.client.query.QueryParser;
import it.cavallium.dbengine.client.query.current.data.QueryParams;
import it.cavallium.dbengine.database.EnglishItalianStopFilter;
import it.cavallium.dbengine.database.LLDocument;
import it.cavallium.dbengine.database.LLKeyScore;
import it.cavallium.dbengine.database.LLLuceneIndex;
import it.cavallium.dbengine.database.LLSearchResult;
import it.cavallium.dbengine.database.LLSearchResultShard;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.LLTerm;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.lucene.AlwaysDirectIOFSDirectory;
import it.cavallium.dbengine.lucene.LuceneUtils;
import it.cavallium.dbengine.lucene.ScheduledTaskLifecycle;
import it.cavallium.dbengine.lucene.searcher.AdaptiveLuceneLocalSearcher;
import it.cavallium.dbengine.lucene.searcher.AdaptiveLuceneMultiSearcher;
import it.cavallium.dbengine.lucene.searcher.LocalQueryParams;
import it.cavallium.dbengine.lucene.searcher.LuceneLocalSearcher;
import it.cavallium.dbengine.lucene.searcher.LuceneMultiSearcher;
import it.cavallium.dbengine.lucene.searcher.LuceneShardSearcher;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.MergeScheduler;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.SnapshotDeletionPolicy;
import org.apache.lucene.misc.store.DirectIODirectory;
import org.apache.lucene.queries.mlt.MoreLikeThis;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.similarities.TFIDFSimilarity;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.store.NRTCachingDirectory;
import org.apache.lucene.util.Constants;
import org.jetbrains.annotations.Nullable;
import org.warp.commonutils.log.Logger;
import org.warp.commonutils.log.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

public class LLLocalLuceneIndex implements LLLuceneIndex {

	protected static final Logger logger = LoggerFactory.getLogger(LLLocalLuceneIndex.class);
	private static final LuceneLocalSearcher localSearcher = new AdaptiveLuceneLocalSearcher();
	/**
	 * Global lucene index scheduler.
	 * There is only a single thread globally to not overwhelm the disk with
	 * concurrent commits or concurrent refreshes.
	 */
	private static final Scheduler luceneHeavyTasksScheduler = Schedulers.newBoundedElastic(1,
			Schedulers.DEFAULT_BOUNDED_ELASTIC_QUEUESIZE,
			"lucene",
			Integer.MAX_VALUE,
			false
	);
	// Scheduler used to get callback values of LuceneStreamSearcher without creating deadlocks
	private final Scheduler luceneSearcherScheduler = Schedulers.newBoundedElastic(
			4,
			Schedulers.DEFAULT_BOUNDED_ELASTIC_QUEUESIZE,
			"lucene-searcher",
			60,
			false
	);
	// Scheduler used to get callback values of LuceneStreamSearcher without creating deadlocks
	private final Scheduler luceneWriterScheduler = Schedulers.newBoundedElastic(
			4,
			Schedulers.DEFAULT_BOUNDED_ELASTIC_QUEUESIZE,
			"lucene-writer",
			60,
			false
	);

	private final String luceneIndexName;
	private final SnapshotDeletionPolicy snapshotter;
	private final IndexWriter indexWriter;
	private final SearcherManager searcherManager;
	private final Directory directory;
	/**
	 * Last snapshot sequence number. 0 is not used
	 */
	private final AtomicLong lastSnapshotSeqNo = new AtomicLong(0);
	/**
	 * LLSnapshot seq no to index commit point
	 */
	private final ConcurrentHashMap<Long, LuceneIndexSnapshot> snapshots = new ConcurrentHashMap<>();
	private final boolean lowMemory;
	private final Similarity similarity;

	private final ScheduledTaskLifecycle scheduledTasksLifecycle;

	public LLLocalLuceneIndex(@Nullable Path luceneBasePath,
			String name,
			IndicizerAnalyzers indicizerAnalyzers,
			IndicizerSimilarities indicizerSimilarities,
			LuceneOptions luceneOptions) throws IOException {
		Path directoryPath;
		if (luceneOptions.inMemory() != (luceneBasePath == null)) {
			throw new IllegalArgumentException();
		} else if (luceneBasePath != null) {
			directoryPath = luceneBasePath.resolve(name + ".lucene.db");
		} else {
			directoryPath = null;
		}
		if (name.length() == 0) {
			throw new IOException("Empty lucene database name");
		}
		if (!MMapDirectory.UNMAP_SUPPORTED) {
			logger.error("Unmap is unsupported, lucene will run slower: {}", MMapDirectory.UNMAP_NOT_SUPPORTED_REASON);
		} else {
			logger.debug("Lucene MMap is supported");
		}
		boolean lowMemory = luceneOptions.lowMemory();
		if (luceneOptions.inMemory()) {
			this.directory = new ByteBuffersDirectory();
		} else {
			Directory directory;
			{
				Directory forcedDirectFsDirectory = null;
				if (luceneOptions.directIOOptions().isPresent()) {
					DirectIOOptions directIOOptions = luceneOptions.directIOOptions().get();
						if (directIOOptions.alwaysForceDirectIO()) {
							try {
								forcedDirectFsDirectory = new AlwaysDirectIOFSDirectory(directoryPath);
							} catch (UnsupportedOperationException ex) {
								logger.warn("Failed to open FSDirectory with DIRECT flag", ex);
							}
						}
				}
				if (forcedDirectFsDirectory != null) {
					directory = forcedDirectFsDirectory;
				} else {
					FSDirectory fsDirectory;
					if (luceneOptions.allowMemoryMapping()) {
						fsDirectory = FSDirectory.open(directoryPath);
					} else {
						fsDirectory = new NIOFSDirectory(directoryPath);
					}
					if (Constants.LINUX || Constants.MAC_OS_X) {
						try {
							int mergeBufferSize;
							long minBytesDirect;
							if (luceneOptions.directIOOptions().isPresent()) {
								var directIOOptions = luceneOptions.directIOOptions().get();
								mergeBufferSize = directIOOptions.mergeBufferSize();
								minBytesDirect = directIOOptions.minBytesDirect();
							} else {
								mergeBufferSize = DirectIODirectory.DEFAULT_MERGE_BUFFER_SIZE;
								minBytesDirect = DirectIODirectory.DEFAULT_MIN_BYTES_DIRECT;
							}
							directory = new DirectIODirectory(fsDirectory, mergeBufferSize, minBytesDirect);
						} catch (UnsupportedOperationException ex) {
							logger.warn("Failed to open FSDirectory with DIRECT flag", ex);
							directory = fsDirectory;
						}
					} else {
						directory = fsDirectory;
					}
				}
			}

			if (luceneOptions.nrtCachingOptions().isPresent()) {
				NRTCachingOptions nrtCachingOptions = luceneOptions.nrtCachingOptions().get();
				directory = new NRTCachingDirectory(directory, nrtCachingOptions.maxMergeSizeMB(), nrtCachingOptions.maxCachedMB());
			}

			this.directory = directory;
		}

		this.luceneIndexName = name;
		this.snapshotter = new SnapshotDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy());
		this.lowMemory = lowMemory;
		this.similarity = LuceneUtils.toPerFieldSimilarityWrapper(indicizerSimilarities);

		IndexWriterConfig indexWriterConfig = new IndexWriterConfig(LuceneUtils.toPerFieldAnalyzerWrapper(indicizerAnalyzers));
		indexWriterConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
		indexWriterConfig.setIndexDeletionPolicy(snapshotter);
		indexWriterConfig.setCommitOnClose(true);
		MergeScheduler mergeScheduler;
		if (lowMemory) {
			mergeScheduler = new SerialMergeScheduler();
		} else {
			var concurrentMergeScheduler = new ConcurrentMergeScheduler();
			concurrentMergeScheduler.enableAutoIOThrottle();
			mergeScheduler = concurrentMergeScheduler;
		}
		indexWriterConfig.setMergeScheduler(mergeScheduler);
		indexWriterConfig.setRAMBufferSizeMB(luceneOptions.indexWriterBufferSize() / 1024D / 1024D);
		indexWriterConfig.setReaderPooling(false);
		indexWriterConfig.setSimilarity(getSimilarity());
		this.indexWriter = new IndexWriter(directory, indexWriterConfig);
		this.searcherManager = new SearcherManager(indexWriter,
				luceneOptions.applyAllDeletes(),
				luceneOptions.writeAllDeletes(),
				new SearcherFactory()
		);

		// Create scheduled tasks lifecycle manager
		this.scheduledTasksLifecycle = new ScheduledTaskLifecycle();

		// Start scheduled tasks
		registerScheduledFixedTask(this::scheduledCommit, luceneOptions.commitDebounceTime());
		registerScheduledFixedTask(this::scheduledQueryRefresh, luceneOptions.queryRefreshDebounceTime());
	}

	private Similarity getSimilarity() {
		return similarity;
	}

	private void registerScheduledFixedTask(Runnable task, Duration duration) {
		new PeriodicTask(task, duration).start();
	}

	private class PeriodicTask implements Runnable {

		private final Runnable task;
		private final Duration duration;
		private volatile boolean cancelled = false;

		public PeriodicTask(Runnable task, Duration duration) {
			this.task = task;
			this.duration = duration;
		}

		public void start() {
			luceneHeavyTasksScheduler.schedule(this,
					duration.toMillis(),
					TimeUnit.MILLISECONDS
			);
		}

		@Override
		public void run() {
			scheduledTasksLifecycle.startScheduledTask();
			try {
				if (scheduledTasksLifecycle.isCancelled() || cancelled) return;
				task.run();
				if (scheduledTasksLifecycle.isCancelled() || cancelled) return;
				luceneHeavyTasksScheduler.schedule(this, duration.toMillis(), TimeUnit.MILLISECONDS);
			} finally {
				scheduledTasksLifecycle.endScheduledTask();
			}
		}

		public void cancel() {
			cancelled = true;
		}
	}

	@Override
	public String getLuceneIndexName() {
		return luceneIndexName;
	}

	@Override
	public Mono<LLSnapshot> takeSnapshot() {
		return takeLuceneSnapshot()
						.flatMap(snapshot -> Mono
								.fromCallable(() -> {
									var snapshotSeqNo = lastSnapshotSeqNo.incrementAndGet();
									this.snapshots.put(snapshotSeqNo, new LuceneIndexSnapshot(snapshot));
									return new LLSnapshot(snapshotSeqNo);
								})
								.subscribeOn(Schedulers.boundedElastic())
						);
	}

	/**
	 * Use internally. This method commits before taking the snapshot if there are no commits in a new database,
	 * avoiding the exception.
	 */
	private Mono<IndexCommit> takeLuceneSnapshot() {
		return Mono
				.fromCallable(snapshotter::snapshot)
				.subscribeOn(Schedulers.boundedElastic())
				.onErrorResume(ex -> Mono
						.defer(() -> {
							if (ex instanceof IllegalStateException && "No index commit to snapshot".equals(ex.getMessage())) {
								return Mono.fromCallable(() -> {
									scheduledTasksLifecycle.startScheduledTask();
									try {
										//noinspection BlockingMethodInNonBlockingContext
										indexWriter.commit();
										//noinspection BlockingMethodInNonBlockingContext
										return snapshotter.snapshot();
									} finally {
										scheduledTasksLifecycle.endScheduledTask();
									}
								}).subscribeOn(luceneHeavyTasksScheduler);
							} else {
								return Mono.error(ex);
							}
						})
				);
	}

	@Override
	public Mono<Void> releaseSnapshot(LLSnapshot snapshot) {
		return Mono.<Void>fromCallable(() -> {
			scheduledTasksLifecycle.startScheduledTask();
			try {
				var indexSnapshot = this.snapshots.remove(snapshot.getSequenceNumber());
				if (indexSnapshot == null) {
					throw new IOException("LLSnapshot " + snapshot.getSequenceNumber() + " not found!");
				}

				indexSnapshot.close();

				var luceneIndexSnapshot = indexSnapshot.getSnapshot();
				snapshotter.release(luceneIndexSnapshot);
				// Delete unused files after releasing the snapshot
				indexWriter.deleteUnusedFiles();
				return null;
			} finally {
				scheduledTasksLifecycle.endScheduledTask();
			}
		}).subscribeOn(Schedulers.boundedElastic());
	}

	@Override
	public Mono<Void> addDocument(LLTerm key, LLDocument doc) {
		return Mono.<Void>fromCallable(() -> {
			scheduledTasksLifecycle.startScheduledTask();
			try {
				//noinspection BlockingMethodInNonBlockingContext
				indexWriter.addDocument(LLUtils.toDocument(doc));
				return null;
			} finally {
				scheduledTasksLifecycle.endScheduledTask();
			}
		}).subscribeOn(luceneWriterScheduler);
	}

	@Override
	public Mono<Void> addDocuments(Flux<Entry<LLTerm, LLDocument>> documents) {
		return documents
				.collectList()
				.flatMap(documentsList -> Mono
						.<Void>fromCallable(() -> {
							scheduledTasksLifecycle.startScheduledTask();
							try {
								//noinspection BlockingMethodInNonBlockingContext
								indexWriter.addDocuments(LLUtils.toDocumentsFromEntries(documentsList));
								return null;
							} finally {
								scheduledTasksLifecycle.endScheduledTask();
							}
						})
						.subscribeOn(luceneWriterScheduler)
				);
	}


	@Override
	public Mono<Void> deleteDocument(LLTerm id) {
		return Mono.<Void>fromCallable(() -> {
			scheduledTasksLifecycle.startScheduledTask();
			try {
				//noinspection BlockingMethodInNonBlockingContext
				indexWriter.deleteDocuments(LLUtils.toTerm(id));
				return null;
			} finally {
				scheduledTasksLifecycle.endScheduledTask();
			}
		}).subscribeOn(luceneWriterScheduler);
	}

	@Override
	public Mono<Void> updateDocument(LLTerm id, LLDocument document) {
		return Mono.<Void>fromCallable(() -> {
			scheduledTasksLifecycle.startScheduledTask();
			try {
				//noinspection BlockingMethodInNonBlockingContext
				indexWriter.updateDocument(LLUtils.toTerm(id), LLUtils.toDocument(document));
			} finally {
				scheduledTasksLifecycle.endScheduledTask();
			}
			return null;
		}).subscribeOn(luceneWriterScheduler);
	}

	@Override
	public Mono<Void> updateDocuments(Mono<Map<LLTerm, LLDocument>> documents) {
		return documents.flatMap(this::updateDocuments).then();
	}

	private Mono<Void> updateDocuments(Map<LLTerm, LLDocument> documentsMap) {
		return Mono
				.<Void>fromCallable(() -> {
					scheduledTasksLifecycle.startScheduledTask();
					try {
						for (Entry<LLTerm, LLDocument> entry : documentsMap.entrySet()) {
							LLTerm key = entry.getKey();
							LLDocument value = entry.getValue();
							//noinspection BlockingMethodInNonBlockingContext
							indexWriter.updateDocument(LLUtils.toTerm(key), LLUtils.toDocument(value));
						}
						return null;
					} finally {
						scheduledTasksLifecycle.endScheduledTask();
					}
				})
				.subscribeOn(luceneWriterScheduler);
	}

	@Override
	public Mono<Void> deleteAll() {
		return Mono.<Void>fromCallable(() -> {
			scheduledTasksLifecycle.startScheduledTask();
			try {
				//noinspection BlockingMethodInNonBlockingContext
				indexWriter.deleteAll();
				//noinspection BlockingMethodInNonBlockingContext
				indexWriter.forceMergeDeletes(true);
				//noinspection BlockingMethodInNonBlockingContext
				indexWriter.commit();
				return null;
			} finally {
				scheduledTasksLifecycle.endScheduledTask();
			}
		}).subscribeOn(luceneHeavyTasksScheduler);
	}

	private Mono<IndexSearcher> acquireSearcherWrapper(LLSnapshot snapshot) {
		return Mono.fromCallable(() -> {
			IndexSearcher indexSearcher;
			if (snapshot == null) {
				indexSearcher = searcherManager.acquire();
				indexSearcher.setSimilarity(getSimilarity());
			} else {
				indexSearcher = resolveSnapshot(snapshot).getIndexSearcher();
			}
			return indexSearcher;
		}).subscribeOn(Schedulers.boundedElastic());
	}

	private Mono<Void> releaseSearcherWrapper(LLSnapshot snapshot, IndexSearcher indexSearcher) {
		return Mono.<Void>fromRunnable(() -> {
			if (snapshot == null) {
				try {
					searcherManager.release(indexSearcher);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}).subscribeOn(Schedulers.boundedElastic());
	}

	@Override
	public Mono<LLSearchResultShard> moreLikeThis(@Nullable LLSnapshot snapshot,
			QueryParams queryParams,
			String keyFieldName,
			Flux<Tuple2<String, Set<String>>> mltDocumentFieldsFlux) {
		return getMoreLikeThisQuery(snapshot, LuceneUtils.toLocalQueryParams(queryParams), mltDocumentFieldsFlux)
				.flatMap(modifiedLocalQuery -> this.acquireSearcherWrapper(snapshot)
						.flatMap(indexSearcher -> {
							Mono<Void> releaseMono = releaseSearcherWrapper(snapshot, indexSearcher);
							return localSearcher
											.collect(indexSearcher, releaseMono, modifiedLocalQuery, keyFieldName, luceneSearcherScheduler)
											.map(result -> new LLSearchResultShard(result.results(), result.totalHitsCount(), result.release()))
											.onErrorResume(ex -> releaseMono.then(Mono.error(ex)));
						})
				);
	}

	public Mono<Void> distributedMoreLikeThis(@Nullable LLSnapshot snapshot,
			QueryParams queryParams,
			Flux<Tuple2<String, Set<String>>> mltDocumentFieldsFlux,
			LuceneShardSearcher shardSearcher) {
		return getMoreLikeThisQuery(snapshot, LuceneUtils.toLocalQueryParams(queryParams), mltDocumentFieldsFlux)
				.flatMap(modifiedLocalQuery -> this.acquireSearcherWrapper(snapshot)
						.flatMap(indexSearcher -> {
							Mono<Void> releaseMono = releaseSearcherWrapper(snapshot, indexSearcher);
							return shardSearcher
									.searchOn(indexSearcher, releaseMono, modifiedLocalQuery, luceneSearcherScheduler)
									.onErrorResume(ex -> releaseMono.then(Mono.error(ex)));
						})
				);
	}

	public Mono<LocalQueryParams> getMoreLikeThisQuery(@Nullable LLSnapshot snapshot,
			LocalQueryParams localQueryParams,
			Flux<Tuple2<String, Set<String>>> mltDocumentFieldsFlux) {
		Query luceneAdditionalQuery;
		try {
			luceneAdditionalQuery = localQueryParams.query();
		} catch (Exception e) {
			return Mono.error(e);
		}
		return mltDocumentFieldsFlux
				.collectMap(Tuple2::getT1, Tuple2::getT2, HashMap::new)
				.flatMap(mltDocumentFields -> {
					mltDocumentFields.entrySet().removeIf(entry -> entry.getValue().isEmpty());
					if (mltDocumentFields.isEmpty()) {
						return Mono.just(new LocalQueryParams(new MatchNoDocsQuery(),
								localQueryParams.offset(),
								localQueryParams.limit(),
								localQueryParams.minCompetitiveScore(),
								localQueryParams.sort(),
								localQueryParams.scoreMode()
						));
					}
					return Mono
							.usingWhen(
									this.acquireSearcherWrapper(snapshot),
									indexSearcher -> Mono
											.fromCallable(() -> {
												var mlt = new MoreLikeThis(indexSearcher.getIndexReader());
												mlt.setAnalyzer(indexWriter.getAnalyzer());
												mlt.setFieldNames(mltDocumentFields.keySet().toArray(String[]::new));
												mlt.setMinTermFreq(1);
												mlt.setMinDocFreq(3);
												mlt.setMaxDocFreqPct(20);
												mlt.setBoost(localQueryParams.scoreMode().needsScores());
												mlt.setStopWords(EnglishItalianStopFilter.getStopWordsString());
												var similarity = getSimilarity();
												if (similarity instanceof TFIDFSimilarity) {
													mlt.setSimilarity((TFIDFSimilarity) similarity);
												} else {
													logger.trace("Using an unsupported similarity algorithm for MoreLikeThis:"
															+ " {}. You must use a similarity instance based on TFIDFSimilarity!", similarity);
												}

												// Get the reference docId and apply it to MoreLikeThis, to generate the query
												@SuppressWarnings({"unchecked", "rawtypes"})
												var mltQuery = mlt.like((Map) mltDocumentFields);
												Query luceneQuery;
												if (!(luceneAdditionalQuery instanceof MatchAllDocsQuery)) {
													luceneQuery = new BooleanQuery.Builder()
															.add(mltQuery, Occur.MUST)
															.add(new ConstantScoreQuery(luceneAdditionalQuery), Occur.MUST)
															.build();
												} else {
													luceneQuery = mltQuery;
												}

												return luceneQuery;
											})
											.subscribeOn(Schedulers.boundedElastic())
											.map(luceneQuery -> new LocalQueryParams(luceneQuery,
													localQueryParams.offset(),
													localQueryParams.limit(),
													localQueryParams.minCompetitiveScore(),
													localQueryParams.sort(),
													localQueryParams.scoreMode()
											)),
									indexSearcher -> releaseSearcherWrapper(snapshot, indexSearcher)
							);
				});
	}

	@Override
	public Mono<LLSearchResultShard> search(@Nullable LLSnapshot snapshot, QueryParams queryParams, String keyFieldName) {
		LocalQueryParams localQueryParams = LuceneUtils.toLocalQueryParams(queryParams);
		return this.acquireSearcherWrapper(snapshot)
				.flatMap(indexSearcher -> {
					Mono<Void> releaseMono = releaseSearcherWrapper(snapshot, indexSearcher);
					return localSearcher
							.collect(indexSearcher, releaseMono, localQueryParams, keyFieldName, luceneSearcherScheduler)
							.map(result -> new LLSearchResultShard(result.results(), result.totalHitsCount(), result.release()))
							.onErrorResume(ex -> releaseMono.then(Mono.error(ex)));
				});
	}

	public Mono<Void> distributedSearch(@Nullable LLSnapshot snapshot,
			QueryParams queryParams,
			LuceneShardSearcher shardSearcher) {
		LocalQueryParams localQueryParams = LuceneUtils.toLocalQueryParams(queryParams);
		return this.acquireSearcherWrapper(snapshot)
				.flatMap(indexSearcher -> {
					Mono<Void> releaseMono = releaseSearcherWrapper(snapshot, indexSearcher);
					return shardSearcher.searchOn(indexSearcher, releaseMono, localQueryParams, luceneSearcherScheduler)
							.onErrorResume(ex -> releaseMono.then(Mono.error(ex)));
				});
	}

	@Override
	public Mono<Void> close() {
		return Mono
				.<Void>fromCallable(() -> {
					logger.debug("Closing IndexWriter...");
					scheduledTasksLifecycle.cancelAndWait();
					//noinspection BlockingMethodInNonBlockingContext
					indexWriter.close();
					//noinspection BlockingMethodInNonBlockingContext
					directory.close();
					logger.debug("IndexWriter closed");
					return null;
				})
				.subscribeOn(luceneHeavyTasksScheduler);
	}

	@Override
	public Mono<Void> flush() {
		return Mono
				.<Void>fromCallable(() -> {
					scheduledTasksLifecycle.startScheduledTask();
					try {
						if (scheduledTasksLifecycle.isCancelled()) return null;
						//noinspection BlockingMethodInNonBlockingContext
						indexWriter.commit();
					} finally {
						scheduledTasksLifecycle.endScheduledTask();
					}
					return null;
				})
				.subscribeOn(luceneHeavyTasksScheduler);
	}

	@Override
	public Mono<Void> refresh(boolean force) {
		return Mono
				.<Void>fromCallable(() -> {
					scheduledTasksLifecycle.startScheduledTask();
					try {
						if (scheduledTasksLifecycle.isCancelled()) return null;
						if (force) {
							if (scheduledTasksLifecycle.isCancelled()) return null;
							//noinspection BlockingMethodInNonBlockingContext
							searcherManager.maybeRefreshBlocking();
						} else {
							//noinspection BlockingMethodInNonBlockingContext
							searcherManager.maybeRefresh();
						}
					} finally {
						scheduledTasksLifecycle.endScheduledTask();
					}
					return null;
				})
				.subscribeOn(luceneHeavyTasksScheduler);
	}

	private void scheduledCommit() {
		try {
			if (indexWriter.hasUncommittedChanges()) {
				indexWriter.commit();
			}
		} catch (IOException ex) {
			ex.printStackTrace();
		}
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

	private LuceneIndexSnapshot resolveSnapshot(@Nullable LLSnapshot snapshot) {
		if (snapshot == null) {
			return null;
		}
		return Objects.requireNonNull(snapshots.get(snapshot.getSequenceNumber()),
				() -> "Can't resolve snapshot " + snapshot.getSequenceNumber()
		);
	}

	@Override
	public boolean isLowMemoryMode() {
		return lowMemory;
	}
}
