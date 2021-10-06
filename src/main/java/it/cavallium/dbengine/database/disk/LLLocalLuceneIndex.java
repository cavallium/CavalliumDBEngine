package it.cavallium.dbengine.database.disk;

import static it.cavallium.dbengine.database.LLUtils.MARKER_LUCENE;
import static it.cavallium.dbengine.lucene.searcher.LLSearchTransformer.NO_TRANSFORMATION;

import io.net5.buffer.api.Send;
import it.cavallium.dbengine.client.DirectIOOptions;
import it.cavallium.dbengine.client.IndicizerAnalyzers;
import it.cavallium.dbengine.client.IndicizerSimilarities;
import it.cavallium.dbengine.client.LuceneOptions;
import it.cavallium.dbengine.client.NRTCachingOptions;
import it.cavallium.dbengine.client.query.current.data.QueryParams;
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
import it.cavallium.dbengine.lucene.searcher.LuceneMultiSearcher;
import it.cavallium.dbengine.lucene.searcher.LLSearchTransformer;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.MergeScheduler;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.SnapshotDeletionPolicy;
import org.apache.lucene.misc.store.DirectIODirectory;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.store.NRTCachingDirectory;
import org.apache.lucene.util.Constants;
import org.jetbrains.annotations.Nullable;
import org.reactivestreams.Publisher;
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
	private static final Scheduler luceneHeavyTasksScheduler = Schedulers.single(Schedulers.boundedElastic());

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
		indexWriterConfig.setRAMBufferSizeMB(luceneOptions.indexWriterBufferSize() / 1024D / 1024D);
		indexWriterConfig.setReaderPooling(false);
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
		return Mono.defer(() -> {
			if (closeRequested.get()) {
				return Mono.error(new IllegalStateException("Lucene index is closed"));
			} else {
				return mono;
			}
		}).doFirst(activeTasks::register).doFinally(s -> activeTasks.arriveAndDeregister());
	}

	@Override
	public Mono<Void> releaseSnapshot(LLSnapshot snapshot) {
		return snapshotsManager.releaseSnapshot(snapshot);
	}

	@Override
	public Mono<Void> addDocument(LLTerm key, LLDocument doc) {
		return Mono.<Void>fromCallable(() -> {
				indexWriter.addDocument(LLUtils.toDocument(doc));
				return null;
		}).subscribeOn(Schedulers.boundedElastic()).transform(this::ensureOpen);
	}

	@Override
	public Mono<Void> addDocuments(Flux<Entry<LLTerm, LLDocument>> documents) {
		return documents
				.collectList()
				.flatMap(documentsList -> Mono
						.<Void>fromCallable(() -> {
							indexWriter.addDocuments(LLUtils.toDocumentsFromEntries(documentsList));
							return null;
						}).subscribeOn(Schedulers.boundedElastic())
				)
				.transform(this::ensureOpen);
	}


	@Override
	public Mono<Void> deleteDocument(LLTerm id) {
		return Mono.<Void>fromCallable(() -> {
			indexWriter.deleteDocuments(LLUtils.toTerm(id));
			return null;
		}).subscribeOn(Schedulers.boundedElastic()).transform(this::ensureOpen);
	}

	@Override
	public Mono<Void> updateDocument(LLTerm id, LLDocument document) {
		return Mono.<Void>fromCallable(() -> {
			indexWriter.updateDocument(LLUtils.toTerm(id), LLUtils.toDocument(document));
			return null;
		}).subscribeOn(Schedulers.boundedElastic()).transform(this::ensureOpen);
	}

	@Override
	public Mono<Void> updateDocuments(Mono<Map<LLTerm, LLDocument>> documents) {
		return documents.flatMap(this::updateDocuments).then();
	}

	private Mono<Void> updateDocuments(Map<LLTerm, LLDocument> documentsMap) {
		return Mono
				.<Void>fromCallable(() -> {
					for (Entry<LLTerm, LLDocument> entry : documentsMap.entrySet()) {
						LLTerm key = entry.getKey();
						LLDocument value = entry.getValue();
						indexWriter.updateDocument(LLUtils.toTerm(key), LLUtils.toDocument(value));
					}
					return null;
				})
				.subscribeOn(Schedulers.boundedElastic())
				.transform(this::ensureOpen);
	}

	@Override
	public Mono<Void> deleteAll() {
		return Mono.<Void>fromCallable(() -> {
			//noinspection BlockingMethodInNonBlockingContext
			indexWriter.deleteAll();
			//noinspection BlockingMethodInNonBlockingContext
			indexWriter.forceMergeDeletes(true);
			//noinspection BlockingMethodInNonBlockingContext
			indexWriter.commit();
			return null;
		}).subscribeOn(luceneHeavyTasksScheduler).transform(this::ensureOpen);
	}

	@Override
	public Mono<Send<LLSearchResultShard>> moreLikeThis(@Nullable LLSnapshot snapshot,
			QueryParams queryParams,
			String keyFieldName,
			Flux<Tuple2<String, Set<String>>> mltDocumentFieldsFlux) {
		LocalQueryParams localQueryParams = LuceneUtils.toLocalQueryParams(queryParams);
		var searcher = this.searcherManager.retrieveSearcher(snapshot);
		var transformer = new MoreLikeThisTransformer(mltDocumentFieldsFlux);

		return localSearcher.collect(searcher, localQueryParams, keyFieldName, transformer).map(resultToReceive -> {
			var result = resultToReceive.receive();
			return new LLSearchResultShard(result.results(), result.totalHitsCount(), result::close).send();
		}).doOnDiscard(Send.class, Send::close);
	}

	@Override
	public Mono<Send<LLSearchResultShard>> search(@Nullable LLSnapshot snapshot, QueryParams queryParams,
			String keyFieldName) {
		LocalQueryParams localQueryParams = LuceneUtils.toLocalQueryParams(queryParams);
		var searcher = searcherManager.retrieveSearcher(snapshot);

		return localSearcher.collect(searcher, localQueryParams, keyFieldName, NO_TRANSFORMATION).map(resultToReceive -> {
			var result = resultToReceive.receive();
			return new LLSearchResultShard(result.results(), result.totalHitsCount(), result::close).send();
		}).doOnDiscard(Send.class, Send::close);
	}

	public Mono<Send<LLIndexSearcher>> retrieveSearcher(@Nullable LLSnapshot snapshot) {
		return searcherManager
				.retrieveSearcher(snapshot)
				.doOnDiscard(Send.class, Send::close);
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
					indexWriter.commit();
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
			if (indexWriter.hasUncommittedChanges()) {
				indexWriter.commit();
			}
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
