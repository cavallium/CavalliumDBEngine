package it.cavallium.dbengine.database.disk;

import static it.cavallium.dbengine.database.LLUtils.MARKER_LUCENE;
import static it.cavallium.dbengine.lucene.searcher.LLSearchTransformer.NO_TRANSFORMATION;

import io.micrometer.core.instrument.MeterRegistry;
import io.net5.buffer.api.Resource;
import io.net5.buffer.api.Send;
import io.net5.buffer.api.internal.ResourceSupport;
import it.cavallium.dbengine.client.DirectIOOptions;
import it.cavallium.dbengine.client.IndicizerAnalyzers;
import it.cavallium.dbengine.client.IndicizerSimilarities;
import it.cavallium.dbengine.client.LuceneOptions;
import it.cavallium.dbengine.client.NRTCachingOptions;
import it.cavallium.dbengine.client.query.QueryParser;
import it.cavallium.dbengine.client.query.current.data.Query;
import it.cavallium.dbengine.client.query.current.data.QueryParams;
import it.cavallium.dbengine.database.LLIndexRequest;
import it.cavallium.dbengine.database.LLSoftUpdateDocument;
import it.cavallium.dbengine.database.LLUpdateDocument;
import it.cavallium.dbengine.database.LLItem;
import it.cavallium.dbengine.database.LLLuceneIndex;
import it.cavallium.dbengine.database.LLSearchResultShard;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.LLTerm;
import it.cavallium.dbengine.database.LLUpdateFields;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.lucene.LuceneHacks;
import it.cavallium.dbengine.lucene.AlwaysDirectIOFSDirectory;
import it.cavallium.dbengine.lucene.LuceneUtils;
import it.cavallium.dbengine.lucene.collector.Buckets;
import it.cavallium.dbengine.lucene.searcher.AdaptiveLocalSearcher;
import it.cavallium.dbengine.lucene.searcher.BucketParams;
import it.cavallium.dbengine.lucene.searcher.DecimalBucketMultiSearcher;
import it.cavallium.dbengine.lucene.searcher.LocalQueryParams;
import it.cavallium.dbengine.lucene.searcher.LocalSearcher;
import it.cavallium.dbengine.lucene.searcher.LLSearchTransformer;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeScheduler;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.SnapshotDeletionPolicy;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.misc.store.DirectIODirectory;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.store.NRTCachingDirectory;
import org.apache.lucene.util.Constants;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.warp.commonutils.functional.IORunnable;
import org.warp.commonutils.log.Logger;
import org.warp.commonutils.log.LoggerFactory;
import org.warp.commonutils.type.ShortNamedThreadFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.function.Tuple2;

public class LLLocalLuceneIndex implements LLLuceneIndex {

	protected static final Logger logger = LoggerFactory.getLogger(LLLocalLuceneIndex.class);
	private final LocalSearcher localSearcher;
	private final DecimalBucketMultiSearcher decimalBucketMultiSearcher = new DecimalBucketMultiSearcher();
	/**
	 * Global lucene index scheduler.
	 * There is only a single thread globally to not overwhelm the disk with
	 * concurrent commits or concurrent refreshes.
	 */
	private static final Scheduler luceneHeavyTasksScheduler = Schedulers.single(Schedulers.boundedElastic());
	private static final ExecutorService SAFE_EXECUTOR = Executors.newCachedThreadPool(new ShortNamedThreadFactory("lucene-index-impl"));

	private final MeterRegistry meterRegistry;
	private final String luceneIndexName;
	private final IndexWriter indexWriter;
	private final SnapshotsManager snapshotsManager;
	private final IndexSearcherManager searcherManager;
	private final PerFieldAnalyzerWrapper luceneAnalyzer;
	private final Similarity luceneSimilarity;
	private final Directory directory;
	private final boolean lowMemory;

	private final Phaser activeTasks = new Phaser(1);
	private final AtomicBoolean closeRequested = new AtomicBoolean();

	public LLLocalLuceneIndex(LLTempLMDBEnv env,
			@Nullable Path luceneBasePath,
			MeterRegistry meterRegistry,
			String name,
			IndicizerAnalyzers indicizerAnalyzers,
			IndicizerSimilarities indicizerSimilarities,
			LuceneOptions luceneOptions,
			@Nullable LuceneHacks luceneHacks) throws IOException {
		this.meterRegistry = meterRegistry;
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
				directory = new NRTCachingDirectory(directory, nrtCachingOptions.maxMergeSizeMB(),
						nrtCachingOptions.maxCachedMB());
			}

			this.directory = directory;
		}

		this.luceneIndexName = name;
		var snapshotter = new SnapshotDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy());
		this.lowMemory = lowMemory;
		this.luceneAnalyzer = LuceneUtils.toPerFieldAnalyzerWrapper(indicizerAnalyzers);
		this.luceneSimilarity = LuceneUtils.toPerFieldSimilarityWrapper(indicizerSimilarities);
		if (luceneHacks != null && luceneHacks.customLocalSearcher() != null) {
			localSearcher = luceneHacks.customLocalSearcher().get();
		} else {
			localSearcher = new AdaptiveLocalSearcher(env);
		}

		var indexWriterConfig = new IndexWriterConfig(luceneAnalyzer);
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
			// false means SSD, true means HDD
			concurrentMergeScheduler.setDefaultMaxMergesAndThreads(false);
			if (luceneOptions.inMemory()) {
				concurrentMergeScheduler.disableAutoIOThrottle();
			} else {
				concurrentMergeScheduler.enableAutoIOThrottle();
			}
			writerSchedulerMaxThreadCount = concurrentMergeScheduler.getMaxThreadCount();
			mergeScheduler = concurrentMergeScheduler;
		}
		logger.trace("WriterSchedulerMaxThreadCount: {}", writerSchedulerMaxThreadCount);
		indexWriterConfig.setMergeScheduler(mergeScheduler);
		if (luceneOptions.indexWriterBufferSize() == -1) {
			//todo: allow to configure maxbuffereddocs fallback
			indexWriterConfig.setMaxBufferedDocs(1000);
			// disable ram buffer size after enabling maxBufferedDocs
			indexWriterConfig.setRAMBufferSizeMB(-1);
		} else {
			indexWriterConfig.setRAMBufferSizeMB(luceneOptions.indexWriterBufferSize() / 1024D / 1024D);
		}
		//indexWriterConfig.setReaderPooling(false);
		indexWriterConfig.setSimilarity(getLuceneSimilarity());
		this.indexWriter = new IndexWriter(directory, indexWriterConfig);
		this.snapshotsManager = new SnapshotsManager(indexWriter, snapshotter);
		this.searcherManager = new CachedIndexSearcherManager(indexWriter,
				snapshotsManager,
				getLuceneSimilarity(),
				luceneOptions.applyAllDeletes(),
				luceneOptions.writeAllDeletes(),
				luceneOptions.queryRefreshDebounceTime()
		);

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
		return luceneIndexName;
	}

	@Override
	public Mono<LLSnapshot> takeSnapshot() {
		return snapshotsManager.takeSnapshot().subscribeOn(luceneHeavyTasksScheduler).transform(this::ensureOpen);
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
		return Mono.<V>create(sink -> {
			var future = SAFE_EXECUTOR.submit(() -> {
				try {
					var result = callable.call();
					if (result != null) {
						sink.success(result);
					} else {
						sink.success();
					}
				} catch (Throwable e) {
					sink.error(e);
				}
			});
			sink.onDispose(() -> future.cancel(false));
		});
	}

	private <V> Mono<V> runSafe(IORunnable runnable) {
		return Mono.create(sink -> {
			var future = SAFE_EXECUTOR.submit(() -> {
				try {
					runnable.run();
					sink.success();
				} catch (Throwable e) {
					sink.error(e);
				}
			});
			sink.onDispose(() -> future.cancel(false));
		});
	}

	@Override
	public Mono<Void> releaseSnapshot(LLSnapshot snapshot) {
		return snapshotsManager.releaseSnapshot(snapshot);
	}

	@Override
	public Mono<Void> addDocument(LLTerm key, LLUpdateDocument doc) {
		return this.<Void>runSafe(() -> indexWriter.addDocument(LLUtils.toDocument(doc))).transform(this::ensureOpen);
	}

	@Override
	public Mono<Void> addDocuments(Flux<Entry<LLTerm, LLUpdateDocument>> documents) {
		return documents
				.collectList()
				.flatMap(documentsList -> this.<Void>runSafe(() -> indexWriter.addDocuments(LLUtils
						.toDocumentsFromEntries(documentsList))))
				.transform(this::ensureOpen);
	}


	@Override
	public Mono<Void> deleteDocument(LLTerm id) {
		return this.<Void>runSafe(() -> indexWriter.deleteDocuments(LLUtils.toTerm(id))).transform(this::ensureOpen);
	}

	@Override
	public Mono<Void> update(LLTerm id, LLIndexRequest request) {
		return this
				.<Void>runSafe(() -> {
					if (request instanceof LLUpdateDocument updateDocument) {
						indexWriter.updateDocument(LLUtils.toTerm(id), LLUtils.toDocument(updateDocument));
					} else if (request instanceof LLSoftUpdateDocument softUpdateDocument) {
						indexWriter.softUpdateDocument(LLUtils.toTerm(id),
								LLUtils.toDocument(softUpdateDocument.items()),
								LLUtils.toFields(softUpdateDocument.softDeleteItems())
						);
					} else if (request instanceof LLUpdateFields updateFields) {
						indexWriter.updateDocValues(LLUtils.toTerm(id), LLUtils.toFields(updateFields.items()));
					} else {
						throw new UnsupportedOperationException("Unexpected request type: " + request);
					}
				})
				.transform(this::ensureOpen);
	}

	@Override
	public Mono<Void> updateDocuments(Mono<Map<LLTerm, LLUpdateDocument>> documents) {
		return documents.flatMap(this::updateDocuments).then();
	}

	private Mono<Void> updateDocuments(Map<LLTerm, LLUpdateDocument> documentsMap) {
		return this.<Void>runSafe(() -> {
			for (Entry<LLTerm, LLUpdateDocument> entry : documentsMap.entrySet()) {
				LLTerm key = entry.getKey();
				LLUpdateDocument value = entry.getValue();
				indexWriter.updateDocument(LLUtils.toTerm(key), LLUtils.toDocument(value));
			}
		}).transform(this::ensureOpen);
	}

	@Override
	public Mono<Void> deleteAll() {
		return this.<Void>runSafe(() -> {
			indexWriter.deleteAll();
			indexWriter.forceMergeDeletes(true);
			indexWriter.commit();
		}).subscribeOn(luceneHeavyTasksScheduler).transform(this::ensureOpen);
	}

	@Override
	public Mono<LLSearchResultShard> moreLikeThis(@Nullable LLSnapshot snapshot,
			QueryParams queryParams,
			String keyFieldName,
			Flux<Tuple2<String, Set<String>>> mltDocumentFieldsFlux) {
		LocalQueryParams localQueryParams = LuceneUtils.toLocalQueryParams(queryParams, luceneAnalyzer);
		var searcher = this.searcherManager.retrieveSearcher(snapshot);
		var transformer = new MoreLikeThisTransformer(mltDocumentFieldsFlux);

		return localSearcher
				.collect(searcher, localQueryParams, keyFieldName, transformer)
				.map(result -> new LLSearchResultShard(result.results(), result.totalHitsCount(), result::close))
				.doOnDiscard(Send.class, Send::close)
				.doOnDiscard(Resource.class, Resource::close);
	}

	@Override
	public Mono<LLSearchResultShard> search(@Nullable LLSnapshot snapshot, QueryParams queryParams,
			String keyFieldName) {
		LocalQueryParams localQueryParams = LuceneUtils.toLocalQueryParams(queryParams, luceneAnalyzer);
		var searcher = searcherManager.retrieveSearcher(snapshot);

		return localSearcher
				.collect(searcher, localQueryParams, keyFieldName, NO_TRANSFORMATION)
				.map(result -> new LLSearchResultShard(result.results(), result.totalHitsCount(), result::close))
				.doOnDiscard(Send.class, Send::close)
				.doOnDiscard(Resource.class, Resource::close);
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

		return decimalBucketMultiSearcher
				.collectMulti(searchers, bucketParams, localQueries, localNormalizationQuery)
				.doOnDiscard(Send.class, Send::close)
				.doOnDiscard(Resource.class, Resource::close);
	}

	public Mono<Send<LLIndexSearcher>> retrieveSearcher(@Nullable LLSnapshot snapshot) {
		return searcherManager
				.retrieveSearcher(snapshot)
				.doOnDiscard(Send.class, Send::close)
				.doOnDiscard(Resource.class, Resource::close);
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
					//noinspection BlockingMethodInNonBlockingContext
					indexWriter.flush();
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
						if (force) {
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
			indexWriter.commit();
		} catch (IOException ex) {
			logger.error(MARKER_LUCENE, "Failed to execute a scheduled commit", ex);
		}
	}

	@Override
	public boolean isLowMemoryMode() {
		return lowMemory;
	}

	private class MoreLikeThisTransformer implements LLSearchTransformer {

		private final Flux<Tuple2<String, Set<String>>> mltDocumentFieldsFlux;

		public MoreLikeThisTransformer(Flux<Tuple2<String, Set<String>>> mltDocumentFieldsFlux) {
			this.mltDocumentFieldsFlux = mltDocumentFieldsFlux;
		}

		@Override
		public Mono<LocalQueryParams> transform(Mono<TransformerInput> inputMono) {
			return inputMono.flatMap(input -> LuceneUtils.getMoreLikeThisQuery(input.indexSearchers(), input.queryParams(),
					luceneAnalyzer, luceneSimilarity, mltDocumentFieldsFlux));
		}
	}
}
