package it.cavallium.dbengine.database.disk;

import it.cavallium.dbengine.database.LLDocument;
import it.cavallium.dbengine.database.LLKeyScore;
import it.cavallium.dbengine.database.LLLuceneIndex;
import it.cavallium.dbengine.database.LLScoreMode;
import it.cavallium.dbengine.database.LLSearchResult;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.LLSort;
import it.cavallium.dbengine.database.LLTerm;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.LuceneUtils;
import it.cavallium.dbengine.database.analyzer.TextFieldsAnalyzer;
import it.cavallium.dbengine.lucene.searcher.AdaptiveStreamSearcher;
import it.cavallium.dbengine.lucene.searcher.LuceneStreamSearcher;
import it.cavallium.dbengine.lucene.searcher.PagedStreamSearcher;
import it.cavallium.dbengine.lucene.serializer.QueryParser;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.SnapshotDeletionPolicy;
import org.apache.lucene.queries.mlt.MoreLikeThis;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.Sort;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.jetbrains.annotations.Nullable;
import org.warp.commonutils.concurrency.executor.ScheduledTaskLifecycle;
import org.warp.commonutils.type.ShortNamedThreadFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.EmissionException;
import reactor.core.publisher.Sinks.EmitResult;
import reactor.core.publisher.Sinks.Many;
import reactor.core.publisher.Sinks.One;
import reactor.core.scheduler.Schedulers;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

public class LLLocalLuceneIndex implements LLLuceneIndex {

	private static final LuceneStreamSearcher streamSearcher = new AdaptiveStreamSearcher();
	/**
	 * Global lucene index scheduler.
	 * There is only a single thread globally to not overwhelm the disk with
	 * parallel commits or parallel refreshes.
	 */
	private static final ScheduledExecutorService scheduler
			= Executors.newSingleThreadScheduledExecutor(new ShortNamedThreadFactory("Lucene"));

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
	 * Snapshot seq no to index commit point
	 */
	private final ConcurrentHashMap<Long, LuceneIndexSnapshot> snapshots = new ConcurrentHashMap<>();
	private final boolean lowMemory;

	private final ScheduledTaskLifecycle scheduledTasksLifecycle;

	public LLLocalLuceneIndex(Path luceneBasePath,
			String name,
			TextFieldsAnalyzer analyzer,
			Duration queryRefreshDebounceTime,
			Duration commitDebounceTime,
			boolean lowMemory) throws IOException {
		if (name.length() == 0) {
			throw new IOException("Empty lucene database name");
		}
		Path directoryPath = luceneBasePath.resolve(name + ".lucene.db");
		this.directory = FSDirectory.open(directoryPath);
		this.luceneIndexName = name;
		this.snapshotter = new SnapshotDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy());
		this.lowMemory = lowMemory;
		IndexWriterConfig indexWriterConfig = new IndexWriterConfig(LuceneUtils.getAnalyzer(analyzer));
		indexWriterConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
		indexWriterConfig.setIndexDeletionPolicy(snapshotter);
		indexWriterConfig.setCommitOnClose(true);
		if (lowMemory) {
			indexWriterConfig.setRAMBufferSizeMB(32);
			indexWriterConfig.setRAMPerThreadHardLimitMB(32);
		} else {
			indexWriterConfig.setRAMBufferSizeMB(128);
			indexWriterConfig.setRAMPerThreadHardLimitMB(512);
		}
		this.indexWriter = new IndexWriter(directory, indexWriterConfig);
		this.searcherManager = new SearcherManager(indexWriter, false, false, null);

		// Create scheduled tasks lifecycle manager
		this.scheduledTasksLifecycle = new ScheduledTaskLifecycle();

		// Start scheduled tasks
		registerScheduledFixedTask(this::scheduledCommit, commitDebounceTime);
		registerScheduledFixedTask(this::scheduledQueryRefresh, queryRefreshDebounceTime);
	}

	private void registerScheduledFixedTask(Runnable task, Duration duration) {
		scheduledTasksLifecycle.registerScheduledTask(scheduler.scheduleAtFixedRate(() -> {
			scheduledTasksLifecycle.startScheduledTask();
			try {
				task.run();
			} finally {
				scheduledTasksLifecycle.endScheduledTask();
			}
		}, duration.toMillis(), duration.toMillis(), TimeUnit.MILLISECONDS));
	}

	@Override
	public String getLuceneIndexName() {
		return luceneIndexName;
	}

	@Override
	public Mono<LLSnapshot> takeSnapshot() {
		return Mono
				.fromCallable(lastSnapshotSeqNo::incrementAndGet)
				.subscribeOn(Schedulers.boundedElastic())
				.flatMap(snapshotSeqNo -> takeLuceneSnapshot()
						.flatMap(snapshot -> Mono
								.fromCallable(() -> {
									this.snapshots.put(snapshotSeqNo, new LuceneIndexSnapshot(snapshot));
									return new LLSnapshot(snapshotSeqNo);
								})
								.subscribeOn(Schedulers.boundedElastic())
						)
				);
	}

	/**
	 * Use internally. This method commits before taking the snapshot if there are no commits in a new database,
	 * avoiding the exception.
	 */
	private Mono<IndexCommit> takeLuceneSnapshot() {
		return Mono.fromCallable(() -> {
			try {
				return snapshotter.snapshot();
			} catch (IllegalStateException ex) {
				if ("No index commit to snapshot".equals(ex.getMessage())) {
					indexWriter.commit();
					return snapshotter.snapshot();
				} else {
					throw ex;
				}
			}
		}).subscribeOn(Schedulers.boundedElastic());
	}

	@Override
	public Mono<Void> releaseSnapshot(LLSnapshot snapshot) {
		return Mono.<Void>fromCallable(() -> {
			var indexSnapshot = this.snapshots.remove(snapshot.getSequenceNumber());
			if (indexSnapshot == null) {
				throw new IOException("Snapshot " + snapshot.getSequenceNumber() + " not found!");
			}

			indexSnapshot.close();

			var luceneIndexSnapshot = indexSnapshot.getSnapshot();
			snapshotter.release(luceneIndexSnapshot);
			// Delete unused files after releasing the snapshot
			indexWriter.deleteUnusedFiles();
			return null;
		}).subscribeOn(Schedulers.boundedElastic());
	}

	@Override
	public Mono<Void> addDocument(LLTerm key, LLDocument doc) {
		return Mono.<Void>fromCallable(() -> {
			indexWriter.addDocument(LLUtils.toDocument(doc));
			return null;
		}).subscribeOn(Schedulers.boundedElastic());
	}

	@Override
	public Mono<Void> addDocuments(Flux<GroupedFlux<LLTerm, LLDocument>> documents) {
		return documents
				.flatMap(group -> group
						.collectList()
						.flatMap(docs -> Mono
								.<Void>fromCallable(() -> {
									indexWriter.addDocuments(LLUtils.toDocuments(docs));
									return null;
								})
								.subscribeOn(Schedulers.boundedElastic()))
				)
				.then();
	}


	@Override
	public Mono<Void> deleteDocument(LLTerm id) {
		return Mono.<Void>fromCallable(() -> {
			indexWriter.deleteDocuments(LLUtils.toTerm(id));
			return null;
		}).subscribeOn(Schedulers.boundedElastic());
	}

	@Override
	public Mono<Void> updateDocument(LLTerm id, LLDocument document) {
		return Mono.<Void>fromCallable(() -> {
			indexWriter.updateDocument(LLUtils.toTerm(id), LLUtils.toDocument(document));
			return null;
		}).subscribeOn(Schedulers.boundedElastic());
	}

	@Override
	public Mono<Void> updateDocuments(Flux<GroupedFlux<LLTerm, LLDocument>> documents) {
		return documents.flatMap(this::updateDocuments).then();
	}

	private Mono<Void> updateDocuments(GroupedFlux<LLTerm, LLDocument> documents) {
		return documents
				.map(LLUtils::toDocument)
				.collectList()
				.flatMap(luceneDocuments -> Mono
						.<Void>fromCallable(() -> {
							indexWriter.updateDocuments(LLUtils.toTerm(documents.key()), luceneDocuments);
							return null;
						})
						.subscribeOn(Schedulers.boundedElastic())
				);
	}

	@Override
	public Mono<Void> deleteAll() {
		return Mono.<Void>fromCallable(() -> {
			indexWriter.deleteAll();
			indexWriter.commit();
			indexWriter.forceMergeDeletes(true);
			indexWriter.flush();
			indexWriter.commit();
			return null;
		}).subscribeOn(Schedulers.boundedElastic());
	}

	private Mono<IndexSearcher> acquireSearcherWrapper(LLSnapshot snapshot) {
		return Mono.fromCallable(() -> {
			if (snapshot == null) {
				return searcherManager.acquire();
			} else {
				return resolveSnapshot(snapshot).getIndexSearcher();
			}
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

	@SuppressWarnings({"Convert2MethodRef", "unchecked", "rawtypes"})
	@Override
	public Mono<LLSearchResult> moreLikeThis(@Nullable LLSnapshot snapshot,
			Flux<Tuple2<String, Set<String>>> mltDocumentFieldsFlux,
			int limit,
			String keyFieldName) {
		return mltDocumentFieldsFlux
				.collectMap(Tuple2::getT1, Tuple2::getT2, HashMap::new)
				.flatMap(mltDocumentFields -> {
					if (mltDocumentFields.isEmpty()) {
						return Mono.just(LLSearchResult.empty());
					}

					return acquireSearcherWrapper(snapshot)
							.flatMap(indexSearcher -> Mono
									.fromCallable(() -> {
										var mlt = new MoreLikeThis(indexSearcher.getIndexReader());
										mlt.setAnalyzer(indexWriter.getAnalyzer());
										mlt.setFieldNames(mltDocumentFields.keySet().toArray(String[]::new));
										mlt.setMinTermFreq(1);
										//mlt.setMinDocFreq(1);
										mlt.setBoost(true);

										// Get the reference doc and apply it to MoreLikeThis, to generate the query
										return mlt.like((Map) mltDocumentFields);
									})
									.subscribeOn(Schedulers.boundedElastic())
									.flatMap(query -> Mono
											.fromCallable(() -> {
												One<Long> totalHitsCountSink = Sinks.one();
												Many<LLKeyScore> topKeysSink = Sinks.many().unicast().onBackpressureBuffer(new ArrayBlockingQueue<>(1000));

												streamSearcher.search(indexSearcher,
														query,
														limit,
														null,
														ScoreMode.COMPLETE,
														keyFieldName,
														keyScore -> {
															EmitResult result = topKeysSink.tryEmitNext(keyScore);
															if (result.isFailure()) {
																throw new EmissionException(result);
															}
														},
														totalHitsCount -> {
															EmitResult result = totalHitsCountSink.tryEmitValue(totalHitsCount);
															if (result.isFailure()) {
																throw new EmissionException(result);
															}
														});

												return new LLSearchResult(totalHitsCountSink.asMono(), Flux.just(topKeysSink.asFlux()));
											}).subscribeOn(Schedulers.boundedElastic())
									).then()
									.materialize()
									.flatMap(value -> releaseSearcherWrapper(snapshot, indexSearcher).thenReturn(value))
									.dematerialize()
							);
				});
	}

	@SuppressWarnings("Convert2MethodRef")
	@Override
	public Mono<LLSearchResult> search(@Nullable LLSnapshot snapshot, String queryString, int limit,
			@Nullable LLSort sort, LLScoreMode scoreMode, String keyFieldName) {

		return acquireSearcherWrapper(snapshot)
				.flatMap(indexSearcher -> Mono
						.fromCallable(() -> {
							Query query = QueryParser.parse(queryString);
							Sort luceneSort = LLUtils.toSort(sort);
							org.apache.lucene.search.ScoreMode luceneScoreMode = LLUtils.toScoreMode(scoreMode);
							return Tuples.of(query, Optional.ofNullable(luceneSort), luceneScoreMode);
						})
						.subscribeOn(Schedulers.boundedElastic())
						.flatMap(tuple -> Mono
								.fromCallable(() -> {
									Query query = tuple.getT1();
									Sort luceneSort = tuple.getT2().orElse(null);
									ScoreMode luceneScoreMode = tuple.getT3();

									One<Long> totalHitsCountSink = Sinks.one();
									Many<LLKeyScore> topKeysSink = Sinks.many().unicast().onBackpressureBuffer(new ArrayBlockingQueue<>(PagedStreamSearcher.MAX_ITEMS_PER_PAGE));

									streamSearcher.search(indexSearcher,
											query,
											limit,
											luceneSort,
											luceneScoreMode,
											keyFieldName,
											keyScore -> {
												EmitResult result = topKeysSink.tryEmitNext(keyScore);
												if (result.isFailure()) {
													throw new EmissionException(result);
												}
											},
											totalHitsCount -> {
												EmitResult result = totalHitsCountSink.tryEmitValue(totalHitsCount);
												if (result.isFailure()) {
													throw new EmissionException(result);
												}
											});

									return new LLSearchResult(totalHitsCountSink.asMono(), Flux.just(topKeysSink.asFlux()));
								}).subscribeOn(Schedulers.boundedElastic())
						)
						.materialize()
						.flatMap(value -> releaseSearcherWrapper(snapshot, indexSearcher).thenReturn(value))
						.dematerialize()
				);
	}

	@Override
	public Mono<Void> close() {
		return Mono
				.<Void>fromCallable(() -> {
					scheduledTasksLifecycle.cancelAndWait();
					indexWriter.close();
					directory.close();
					return null;
				})
				.subscribeOn(Schedulers.boundedElastic());
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
