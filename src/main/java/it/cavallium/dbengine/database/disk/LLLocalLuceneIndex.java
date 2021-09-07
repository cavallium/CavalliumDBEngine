package it.cavallium.dbengine.database.disk;

import it.cavallium.dbengine.client.DirectIOOptions;
import it.cavallium.dbengine.client.IndicizerAnalyzers;
import it.cavallium.dbengine.client.IndicizerSimilarities;
import it.cavallium.dbengine.client.LuceneOptions;
import it.cavallium.dbengine.client.NRTCachingOptions;
import it.cavallium.dbengine.client.query.current.data.QueryParams;
import it.cavallium.dbengine.database.EnglishItalianStopFilter;
import it.cavallium.dbengine.database.LLDocument;
import it.cavallium.dbengine.database.LLLuceneIndex;
import it.cavallium.dbengine.database.LLSearchResultShard;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.LLTerm;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.lucene.AlwaysDirectIOFSDirectory;
import it.cavallium.dbengine.lucene.LuceneUtils;
import it.cavallium.dbengine.lucene.searcher.AdaptiveLuceneLocalSearcher;
import it.cavallium.dbengine.lucene.searcher.LocalQueryParams;
import it.cavallium.dbengine.lucene.searcher.LuceneLocalSearcher;
import it.cavallium.dbengine.lucene.searcher.LuceneShardSearcher;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.index.ConcurrentMergeScheduler;
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
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
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
			true
	);
	// Scheduler used to get callback values of LuceneStreamSearcher without creating deadlocks
	protected static final Scheduler luceneSearcherScheduler = Schedulers.newBoundedElastic(
			4,
			Schedulers.DEFAULT_BOUNDED_ELASTIC_QUEUESIZE,
			"lucene-searcher",
			60,
			true
	);
	// Scheduler used to get callback values of LuceneStreamSearcher without creating deadlocks
	private static final Scheduler luceneWriterScheduler = Schedulers.boundedElastic();

	private final String luceneIndexName;
	private final IndexWriter indexWriter;
	private final SnapshotsManager snapshotsManager;
	private final CachedIndexSearcherManager searcherManager;
	private final Similarity similarity;
	private final Directory directory;
	private final boolean lowMemory;

	private final Phaser activeTasks = new Phaser(1);

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
		var snapshotter = new SnapshotDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy());
		this.lowMemory = lowMemory;
		this.similarity = LuceneUtils.toPerFieldSimilarityWrapper(indicizerSimilarities);

		IndexWriterConfig indexWriterConfig = new IndexWriterConfig(LuceneUtils.toPerFieldAnalyzerWrapper(indicizerAnalyzers));
		indexWriterConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
		indexWriterConfig.setIndexDeletionPolicy(snapshotter);
		indexWriterConfig.setCommitOnClose(true);
		int writerSchedulerMaxThreadCount;
		MergeScheduler mergeScheduler;
		if (lowMemory) {
			mergeScheduler = new SerialMergeScheduler();
			writerSchedulerMaxThreadCount = 1;
		} else {
			var concurrentMergeScheduler = new ConcurrentMergeScheduler();
			concurrentMergeScheduler.setDefaultMaxMergesAndThreads(false);
			if (luceneOptions.inMemory()) {
				concurrentMergeScheduler.disableAutoIOThrottle();
			} else {
				concurrentMergeScheduler.enableAutoIOThrottle();
			}
			writerSchedulerMaxThreadCount = concurrentMergeScheduler.getMaxThreadCount();
			mergeScheduler = concurrentMergeScheduler;
		}
		indexWriterConfig.setMergeScheduler(mergeScheduler);
		indexWriterConfig.setRAMBufferSizeMB(luceneOptions.indexWriterBufferSize() / 1024D / 1024D);
		indexWriterConfig.setReaderPooling(false);
		indexWriterConfig.setSimilarity(getSimilarity());
		this.indexWriter = new IndexWriter(directory, indexWriterConfig);
		this.snapshotsManager = new SnapshotsManager(indexWriter, snapshotter);
		this.searcherManager = new CachedIndexSearcherManager(indexWriter,
				snapshotsManager,
				getSimilarity(),
				luceneOptions.applyAllDeletes(),
				luceneOptions.writeAllDeletes(),
				luceneOptions.queryRefreshDebounceTime()
		);

		// Start scheduled tasks
		var commitMillis = luceneOptions.commitDebounceTime().toMillis();
		luceneHeavyTasksScheduler.schedulePeriodically(this::scheduledCommit, commitMillis, commitMillis,
				TimeUnit.MILLISECONDS);
	}

	private Similarity getSimilarity() {
		return similarity;
	}

	@Override
	public String getLuceneIndexName() {
		return luceneIndexName;
	}

	@Override
	public Mono<LLSnapshot> takeSnapshot() {
		return snapshotsManager.takeSnapshot().subscribeOn(luceneHeavyTasksScheduler);
	}

	@Override
	public Mono<Void> releaseSnapshot(LLSnapshot snapshot) {
		return snapshotsManager.releaseSnapshot(snapshot);
	}

	@Override
	public Mono<Void> addDocument(LLTerm key, LLDocument doc) {
		return Mono.<Void>fromCallable(() -> {
			activeTasks.register();
			try {
				//noinspection BlockingMethodInNonBlockingContext
				indexWriter.addDocument(LLUtils.toDocument(doc));
				return null;
			} finally {
				activeTasks.arriveAndDeregister();
			}
		}).subscribeOn(luceneWriterScheduler);
	}

	@Override
	public Mono<Void> addDocuments(Flux<Entry<LLTerm, LLDocument>> documents) {
		return documents
				.collectList()
				.publishOn(luceneWriterScheduler)
				.flatMap(documentsList -> Mono
						.fromCallable(() -> {
							activeTasks.register();
							try {
								indexWriter.addDocuments(LLUtils.toDocumentsFromEntries(documentsList));
								return null;
							} finally {
								activeTasks.arriveAndDeregister();
							}
						})
				);
	}


	@Override
	public Mono<Void> deleteDocument(LLTerm id) {
		return Mono.<Void>fromCallable(() -> {
			activeTasks.register();
			try {
				//noinspection BlockingMethodInNonBlockingContext
				indexWriter.deleteDocuments(LLUtils.toTerm(id));
				return null;
			} finally {
				activeTasks.arriveAndDeregister();
			}
		}).subscribeOn(luceneWriterScheduler);
	}

	@Override
	public Mono<Void> updateDocument(LLTerm id, LLDocument document) {
		return Mono.<Void>fromCallable(() -> {
			activeTasks.register();
			try {
				//noinspection BlockingMethodInNonBlockingContext
				indexWriter.updateDocument(LLUtils.toTerm(id), LLUtils.toDocument(document));
			} finally {
				activeTasks.arriveAndDeregister();
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
					activeTasks.register();
					try {
						for (Entry<LLTerm, LLDocument> entry : documentsMap.entrySet()) {
							LLTerm key = entry.getKey();
							LLDocument value = entry.getValue();
							//noinspection BlockingMethodInNonBlockingContext
							indexWriter.updateDocument(LLUtils.toTerm(key), LLUtils.toDocument(value));
						}
						return null;
					} finally {
						activeTasks.arriveAndDeregister();
					}
				})
				.subscribeOn(luceneWriterScheduler);
	}

	@Override
	public Mono<Void> deleteAll() {
		return Mono.<Void>fromCallable(() -> {
			activeTasks.register();
			try {
				//noinspection BlockingMethodInNonBlockingContext
				indexWriter.deleteAll();
				//noinspection BlockingMethodInNonBlockingContext
				indexWriter.forceMergeDeletes(true);
				//noinspection BlockingMethodInNonBlockingContext
				indexWriter.commit();
				return null;
			} finally {
				activeTasks.arriveAndDeregister();
			}
		}).subscribeOn(luceneHeavyTasksScheduler);
	}

	@Override
	public Mono<LLSearchResultShard> moreLikeThis(@Nullable LLSnapshot snapshot,
			QueryParams queryParams,
			String keyFieldName,
			Flux<Tuple2<String, Set<String>>> mltDocumentFieldsFlux) {
		return getMoreLikeThisQuery(snapshot, LuceneUtils.toLocalQueryParams(queryParams), mltDocumentFieldsFlux)
				.flatMap(modifiedLocalQuery -> searcherManager.captureIndexSearcher(snapshot)
						.flatMap(indexSearcher -> {
							Mono<Void> releaseMono = searcherManager.releaseUsedIndexSearcher(indexSearcher);
							return localSearcher
											.collect(indexSearcher.getIndexSearcher(), releaseMono, modifiedLocalQuery, keyFieldName, luceneSearcherScheduler)
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
				.flatMap(modifiedLocalQuery -> searcherManager.captureIndexSearcher(snapshot)
						.flatMap(indexSearcher -> {
							Mono<Void> releaseMono = searcherManager.releaseUsedIndexSearcher(indexSearcher);
							return shardSearcher
									.searchOn(indexSearcher.getIndexSearcher(), releaseMono, modifiedLocalQuery, luceneSearcherScheduler)
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
					return this.searcherManager.search(snapshot, indexSearcher -> Mono.fromCallable(() -> {
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
					)));
				});
	}

	@Override
	public Mono<LLSearchResultShard> search(@Nullable LLSnapshot snapshot, QueryParams queryParams, String keyFieldName) {
		LocalQueryParams localQueryParams = LuceneUtils.toLocalQueryParams(queryParams);
		return searcherManager.captureIndexSearcher(snapshot).flatMap(indexSearcher -> {
			Mono<Void> releaseMono = searcherManager.releaseUsedIndexSearcher(indexSearcher);
			return localSearcher
					.collect(indexSearcher.getIndexSearcher(), releaseMono, localQueryParams, keyFieldName, luceneSearcherScheduler)
					.map(result -> new LLSearchResultShard(result.results(), result.totalHitsCount(), result.release()))
					.onErrorResume(ex -> releaseMono.then(Mono.error(ex)));
		});
	}

	public Mono<Void> distributedSearch(@Nullable LLSnapshot snapshot,
			QueryParams queryParams,
			LuceneShardSearcher shardSearcher) {
		LocalQueryParams localQueryParams = LuceneUtils.toLocalQueryParams(queryParams);
		return searcherManager.captureIndexSearcher(snapshot)
				.flatMap(indexSearcher -> {
					Mono<Void> releaseMono = searcherManager.releaseUsedIndexSearcher(indexSearcher);
					return shardSearcher.searchOn(indexSearcher.getIndexSearcher(), releaseMono, localQueryParams, luceneSearcherScheduler)
							.onErrorResume(ex -> releaseMono.then(Mono.error(ex)));
				});
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
					logger.info("Closing IndexWriter...");
					//noinspection BlockingMethodInNonBlockingContext
					indexWriter.close();
					//noinspection BlockingMethodInNonBlockingContext
					directory.close();
					logger.info("IndexWriter closed");
					return null;
				}).subscribeOn(luceneHeavyTasksScheduler));
	}

	@Override
	public Mono<Void> flush() {
		return Mono
				.<Void>fromCallable(() -> {
					activeTasks.register();
					try {
						if (activeTasks.isTerminated()) return null;
						//noinspection BlockingMethodInNonBlockingContext
						indexWriter.commit();
					} finally {
						activeTasks.arriveAndDeregister();
					}
					return null;
				})
				.subscribeOn(luceneHeavyTasksScheduler);
	}

	@Override
	public Mono<Void> refresh(boolean force) {
		return Mono
				.<Void>fromCallable(() -> {
					activeTasks.register();
					try {
						if (activeTasks.isTerminated()) return null;
						if (force) {
							if (activeTasks.isTerminated()) return null;
							//noinspection BlockingMethodInNonBlockingContext
							searcherManager.maybeRefreshBlocking();
						} else {
							//noinspection BlockingMethodInNonBlockingContext
							searcherManager.maybeRefresh();
						}
					} finally {
						activeTasks.arriveAndDeregister();
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

	@Override
	public boolean isLowMemoryMode() {
		return lowMemory;
	}
}
