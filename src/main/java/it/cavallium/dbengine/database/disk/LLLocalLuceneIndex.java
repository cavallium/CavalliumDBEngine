package it.cavallium.dbengine.database.disk;

import static it.cavallium.dbengine.lucene.LuceneUtils.checkScoringArgumentsValidity;

import it.cavallium.dbengine.database.LLDocument;
import it.cavallium.dbengine.database.LLKeyScore;
import it.cavallium.dbengine.database.LLLuceneIndex;
import it.cavallium.dbengine.database.LLScoreMode;
import it.cavallium.dbengine.database.LLSearchCollectionStatisticsGetter;
import it.cavallium.dbengine.database.LLSearchResult;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.LLSort;
import it.cavallium.dbengine.database.LLTerm;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.lucene.LuceneUtils;
import it.cavallium.dbengine.lucene.ScheduledTaskLifecycle;
import it.cavallium.dbengine.lucene.analyzer.TextFieldsAnalyzer;
import it.cavallium.dbengine.lucene.analyzer.TextFieldsSimilarity;
import it.cavallium.dbengine.lucene.searcher.AdaptiveStreamSearcher;
import it.cavallium.dbengine.lucene.searcher.AllowOnlyQueryParsingCollectorStreamSearcher;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.math3.exception.NumberIsTooLargeException;
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
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.EmissionException;
import reactor.core.publisher.Sinks.EmitResult;
import reactor.core.publisher.Sinks.Empty;
import reactor.core.publisher.Sinks.Many;
import reactor.core.publisher.Sinks.One;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

public class LLLocalLuceneIndex implements LLLuceneIndex {

	private static final LuceneStreamSearcher streamSearcher = new AdaptiveStreamSearcher();
	private static final AllowOnlyQueryParsingCollectorStreamSearcher allowOnlyQueryParsingCollectorStreamSearcher = new AllowOnlyQueryParsingCollectorStreamSearcher();
	/**
	 * Global lucene index scheduler.
	 * There is only a single thread globally to not overwhelm the disk with
	 * concurrent commits or concurrent refreshes.
	 */
	private static final Scheduler luceneBlockingScheduler = Schedulers.newBoundedElastic(1,
			Schedulers.DEFAULT_BOUNDED_ELASTIC_QUEUESIZE,
			"Lucene",
			120,
			true
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
	 * Snapshot seq no to index commit point
	 */
	private final ConcurrentHashMap<Long, LuceneIndexSnapshot> snapshots = new ConcurrentHashMap<>();
	private final boolean lowMemory;
	private final TextFieldsSimilarity similarity;

	private final ScheduledTaskLifecycle scheduledTasksLifecycle;
	private final @Nullable LLSearchCollectionStatisticsGetter distributedCollectionStatisticsGetter;

	public LLLocalLuceneIndex(Path luceneBasePath,
			String name,
			TextFieldsAnalyzer analyzer,
			TextFieldsSimilarity similarity,
			Duration queryRefreshDebounceTime,
			Duration commitDebounceTime,
			boolean lowMemory,
			@Nullable LLSearchCollectionStatisticsGetter distributedCollectionStatisticsGetter) throws IOException {
		if (name.length() == 0) {
			throw new IOException("Empty lucene database name");
		}
		Path directoryPath = luceneBasePath.resolve(name + ".lucene.db");
		this.directory = FSDirectory.open(directoryPath);
		this.luceneIndexName = name;
		this.snapshotter = new SnapshotDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy());
		this.lowMemory = lowMemory;
		this.similarity = similarity;
		this.distributedCollectionStatisticsGetter = distributedCollectionStatisticsGetter;
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
		indexWriterConfig.setSimilarity(getSimilarity());
		this.indexWriter = new IndexWriter(directory, indexWriterConfig);
		this.searcherManager = new SearcherManager(indexWriter, false, false, null);

		// Create scheduled tasks lifecycle manager
		this.scheduledTasksLifecycle = new ScheduledTaskLifecycle();

		// Start scheduled tasks
		registerScheduledFixedTask(this::scheduledCommit, commitDebounceTime);
		registerScheduledFixedTask(this::scheduledQueryRefresh, queryRefreshDebounceTime);
	}

	private Similarity getSimilarity() {
		return LuceneUtils.getSimilarity(similarity);
	}

	private void registerScheduledFixedTask(Runnable task, Duration duration) {
		scheduledTasksLifecycle.registerScheduledTask(luceneBlockingScheduler.schedulePeriodically(() -> {
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
				.subscribeOn(luceneBlockingScheduler)
				.flatMap(snapshotSeqNo -> takeLuceneSnapshot()
						.flatMap(snapshot -> Mono
								.fromCallable(() -> {
									this.snapshots.put(snapshotSeqNo, new LuceneIndexSnapshot(snapshot));
									return new LLSnapshot(snapshotSeqNo);
								})
								.subscribeOn(luceneBlockingScheduler)
						)
				);
	}

	/**
	 * Use internally. This method commits before taking the snapshot if there are no commits in a new database,
	 * avoiding the exception.
	 */
	private Mono<IndexCommit> takeLuceneSnapshot() {
		return Mono
				.fromCallable(() -> {
					try {
						//noinspection BlockingMethodInNonBlockingContext
						return snapshotter.snapshot();
					} catch (IllegalStateException ex) {
						if ("No index commit to snapshot".equals(ex.getMessage())) {
							//noinspection BlockingMethodInNonBlockingContext
							indexWriter.commit();
							//noinspection BlockingMethodInNonBlockingContext
							return snapshotter.snapshot();
						} else {
							throw ex;
						}
					}
				})
				.subscribeOn(luceneBlockingScheduler);
	}

	@Override
	public Mono<Void> releaseSnapshot(LLSnapshot snapshot) {
		return Mono.<Void>fromCallable(() -> {
			var indexSnapshot = this.snapshots.remove(snapshot.getSequenceNumber());
			if (indexSnapshot == null) {
				throw new IOException("Snapshot " + snapshot.getSequenceNumber() + " not found!");
			}

			//noinspection BlockingMethodInNonBlockingContext
			indexSnapshot.close();

			var luceneIndexSnapshot = indexSnapshot.getSnapshot();
			//noinspection BlockingMethodInNonBlockingContext
			snapshotter.release(luceneIndexSnapshot);
			// Delete unused files after releasing the snapshot
			//noinspection BlockingMethodInNonBlockingContext
			indexWriter.deleteUnusedFiles();
			return null;
		}).subscribeOn(luceneBlockingScheduler);
	}

	@Override
	public Mono<Void> addDocument(LLTerm key, LLDocument doc) {
		return Mono.<Void>fromCallable(() -> {
			//noinspection BlockingMethodInNonBlockingContext
			indexWriter.addDocument(LLUtils.toDocument(doc));
			return null;
		}).subscribeOn(luceneBlockingScheduler);
	}

	@Override
	public Mono<Void> addDocuments(Flux<GroupedFlux<LLTerm, LLDocument>> documents) {
		return documents
				.flatMap(group -> group
						.collectList()
						.flatMap(docs -> Mono
								.<Void>fromCallable(() -> {
									//noinspection BlockingMethodInNonBlockingContext
									indexWriter.addDocuments(LLUtils.toDocuments(docs));
									return null;
								})
								.subscribeOn(luceneBlockingScheduler))
				)
				.then();
	}


	@Override
	public Mono<Void> deleteDocument(LLTerm id) {
		return Mono.<Void>fromCallable(() -> {
			//noinspection BlockingMethodInNonBlockingContext
			indexWriter.deleteDocuments(LLUtils.toTerm(id));
			return null;
		}).subscribeOn(luceneBlockingScheduler);
	}

	@Override
	public Mono<Void> updateDocument(LLTerm id, LLDocument document) {
		return Mono.<Void>fromCallable(() -> {
			//noinspection BlockingMethodInNonBlockingContext
			indexWriter.updateDocument(LLUtils.toTerm(id), LLUtils.toDocument(document));
			return null;
		}).subscribeOn(luceneBlockingScheduler);
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
							//noinspection BlockingMethodInNonBlockingContext
							indexWriter.updateDocuments(LLUtils.toTerm(documents.key()), luceneDocuments);
							return null;
						})
						.subscribeOn(luceneBlockingScheduler)
				);
	}

	@Override
	public Mono<Void> deleteAll() {
		return Mono.<Void>fromCallable(() -> {
			//noinspection BlockingMethodInNonBlockingContext
			indexWriter.deleteAll();
			//noinspection BlockingMethodInNonBlockingContext
			indexWriter.commit();
			//noinspection BlockingMethodInNonBlockingContext
			indexWriter.forceMergeDeletes(true);
			//noinspection BlockingMethodInNonBlockingContext
			indexWriter.flush();
			//noinspection BlockingMethodInNonBlockingContext
			indexWriter.commit();
			return null;
		}).subscribeOn(luceneBlockingScheduler);
	}

	private Mono<IndexSearcher> acquireSearcherWrapper(LLSnapshot snapshot, boolean distributedPre, long actionId) {
		return Mono.fromCallable(() -> {
			IndexSearcher indexSearcher;
			if (snapshot == null) {
				//noinspection BlockingMethodInNonBlockingContext
				indexSearcher = searcherManager.acquire();
				indexSearcher.setSimilarity(getSimilarity());
			} else {
				indexSearcher = resolveSnapshot(snapshot).getIndexSearcher();
			}
			if (distributedCollectionStatisticsGetter != null && actionId != -1) {
				return new LLIndexSearcherWithCustomCollectionStatistics(indexSearcher,
						distributedCollectionStatisticsGetter,
						distributedPre,
						actionId
				);
			} else {
				return indexSearcher;
			}
		}).subscribeOn(luceneBlockingScheduler);
	}

	private Mono<Void> releaseSearcherWrapper(LLSnapshot snapshot, IndexSearcher indexSearcher) {
		return Mono.<Void>fromRunnable(() -> {
			if (snapshot == null) {
				try {
					//noinspection BlockingMethodInNonBlockingContext
					searcherManager.release(indexSearcher);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}).subscribeOn(luceneBlockingScheduler);
	}

	@Override
	public Mono<LLSearchResult> moreLikeThis(@Nullable LLSnapshot snapshot,
			Flux<Tuple2<String, Set<String>>> mltDocumentFieldsFlux,
			long limit,
			@Nullable Float minCompetitiveScore,
			String keyFieldName) {
		return moreLikeThis(snapshot, mltDocumentFieldsFlux, limit, minCompetitiveScore, keyFieldName, false, 0, 1);
	}

	public Mono<LLSearchResult> distributedMoreLikeThis(@Nullable LLSnapshot snapshot,
			Flux<Tuple2<String, Set<String>>> mltDocumentFieldsFlux,
			long limit,
			@Nullable Float minCompetitiveScore,
			String keyFieldName,
			long actionId,
			int scoreDivisor) {
		return moreLikeThis(snapshot, mltDocumentFieldsFlux, limit, minCompetitiveScore, keyFieldName, false, actionId, scoreDivisor);
	}

	public Mono<Void> distributedPreMoreLikeThis(@Nullable LLSnapshot snapshot,
			Flux<Tuple2<String, Set<String>>> mltDocumentFieldsFlux,
			@Nullable Float minCompetitiveScore,
			String keyFieldName, long actionId) {
		return moreLikeThis(snapshot, mltDocumentFieldsFlux, -1, minCompetitiveScore, keyFieldName, true, actionId, 1)
				.flatMap(LLSearchResult::completion);
	}

	@SuppressWarnings({"Convert2MethodRef", "unchecked", "rawtypes"})
	private Mono<LLSearchResult> moreLikeThis(@Nullable LLSnapshot snapshot,
			Flux<Tuple2<String, Set<String>>> mltDocumentFieldsFlux,
			long limit,
			@Nullable Float minCompetitiveScore,
			String keyFieldName,
			boolean doDistributedPre,
			long actionId,
			int scoreDivisor) {
		return mltDocumentFieldsFlux
				.collectMap(Tuple2::getT1, Tuple2::getT2, HashMap::new)
				.flatMap(mltDocumentFields -> {
					if (mltDocumentFields.isEmpty()) {
						return Mono.just(LLSearchResult.empty());
					}

					return acquireSearcherWrapper(snapshot, doDistributedPre, actionId)
							.flatMap(indexSearcher -> Mono
									.fromCallable(() -> {
										var mlt = new MoreLikeThis(indexSearcher.getIndexReader());
										mlt.setAnalyzer(indexWriter.getAnalyzer());
										mlt.setFieldNames(mltDocumentFields.keySet().toArray(String[]::new));
										mlt.setMinTermFreq(1);
										//mlt.setMinDocFreq(1);
										mlt.setBoost(true);

										// Get the reference doc and apply it to MoreLikeThis, to generate the query
										//noinspection BlockingMethodInNonBlockingContext
										return mlt.like((Map) mltDocumentFields);
									})
									.subscribeOn(luceneBlockingScheduler)
									.flatMap(query -> Mono
											.fromCallable(() -> {
												One<Long> totalHitsCountSink = Sinks.one();
												Many<LLKeyScore> topKeysSink = Sinks
														.many()
														.unicast()
														.onBackpressureBuffer(new ArrayBlockingQueue<>(1000));
												Empty<Void> completeSink = Sinks.empty();

												Schedulers.boundedElastic().schedule(() -> {
													try {
														if (doDistributedPre) {
															allowOnlyQueryParsingCollectorStreamSearcher.search(indexSearcher, query);
															totalHitsCountSink.tryEmitValue(0L);
														} else {
															if (limit > Integer.MAX_VALUE) {
																throw new NumberIsTooLargeException(limit, Integer.MAX_VALUE, true);
															}
															streamSearcher.search(indexSearcher,
																	query,
																	(int) limit,
																	null,
																	ScoreMode.TOP_SCORES,
																	minCompetitiveScore,
																	keyFieldName,
																	keyScore -> {
																		EmitResult result = topKeysSink.tryEmitNext(fixKeyScore(keyScore, scoreDivisor));
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
														}
														topKeysSink.tryEmitComplete();
														completeSink.tryEmitEmpty();
													} catch (IOException e) {
														topKeysSink.tryEmitError(e);
														totalHitsCountSink.tryEmitError(e);
														completeSink.tryEmitError(e);
													}
												});

												return new LLSearchResult(totalHitsCountSink.asMono(), Flux.just(topKeysSink.asFlux()));
											}).subscribeOn(luceneBlockingScheduler)
									).then()
									.materialize()
									.flatMap(signal -> {
										if (signal.isOnComplete() || signal.isOnError()) {
											return releaseSearcherWrapper(snapshot, indexSearcher).thenReturn(signal);
										} else {
											return Mono.just(signal);
										}
									})
									.dematerialize()
							);
				});
	}

	private LLKeyScore fixKeyScore(LLKeyScore keyScore, int scoreDivisor) {
		return scoreDivisor == 1 ? keyScore : new LLKeyScore(keyScore.getKey(), keyScore.getScore() / (float) scoreDivisor);
	}

	@Override
	public Mono<LLSearchResult> search(@Nullable LLSnapshot snapshot, it.cavallium.dbengine.lucene.serializer.Query query, long limit,
			@Nullable LLSort sort, @NotNull LLScoreMode scoreMode, @Nullable Float minCompetitiveScore, String keyFieldName) {
		return search(snapshot, query, limit, sort, scoreMode, minCompetitiveScore,
				keyFieldName, false, 0, 1);
	}

	public Mono<LLSearchResult> distributedSearch(@Nullable LLSnapshot snapshot, it.cavallium.dbengine.lucene.serializer.Query query, long limit,
			@Nullable LLSort sort, @NotNull LLScoreMode scoreMode, @Nullable Float minCompetitiveScore, String keyFieldName, long actionId, int scoreDivisor) {
		return search(snapshot, query, limit, sort, scoreMode, minCompetitiveScore,
				keyFieldName, false, actionId, scoreDivisor);
	}

	public Mono<Void> distributedPreSearch(@Nullable LLSnapshot snapshot, it.cavallium.dbengine.lucene.serializer.Query query,
			@Nullable LLSort sort, @NotNull LLScoreMode scoreMode, @Nullable Float minCompetitiveScore, String keyFieldName, long actionId) {
		return this
				.search(snapshot, query, -1, sort, scoreMode,
						minCompetitiveScore, keyFieldName, true, actionId, 1)
				.flatMap(LLSearchResult::completion);
	}

	@SuppressWarnings("Convert2MethodRef")
	private Mono<LLSearchResult> search(@Nullable LLSnapshot snapshot,
			it.cavallium.dbengine.lucene.serializer.Query query, long limit,
			@Nullable LLSort sort, @NotNull LLScoreMode scoreMode, @Nullable Float minCompetitiveScore, String keyFieldName,
			boolean doDistributedPre, long actionId, int scoreDivisor) {
		return acquireSearcherWrapper(snapshot, doDistributedPre, actionId)
				.flatMap(indexSearcher -> Mono
						.fromCallable(() -> {
							Objects.requireNonNull(scoreMode, "ScoreMode must not be null");
							checkScoringArgumentsValidity(sort, scoreMode);
							Query luceneQuery = QueryParser.parse(query);
							Sort luceneSort = LLUtils.toSort(sort);
							org.apache.lucene.search.ScoreMode luceneScoreMode = LLUtils.toScoreMode(scoreMode);
							return Tuples.of(luceneQuery, Optional.ofNullable(luceneSort), luceneScoreMode);
						})
						.subscribeOn(luceneBlockingScheduler)
						.flatMap(tuple -> Mono
								.fromCallable(() -> {
									Query luceneQuery = tuple.getT1();
									Sort luceneSort = tuple.getT2().orElse(null);
									ScoreMode luceneScoreMode = tuple.getT3();

									One<Long> totalHitsCountSink = Sinks.one();
									Many<LLKeyScore> topKeysSink = Sinks
											.many()
											.unicast()
											.onBackpressureBuffer(new ArrayBlockingQueue<>(PagedStreamSearcher.MAX_ITEMS_PER_PAGE));
									Schedulers.boundedElastic().schedule(() -> {
										try {
											if (doDistributedPre) {
												allowOnlyQueryParsingCollectorStreamSearcher.search(indexSearcher, luceneQuery);
												totalHitsCountSink.tryEmitValue(0L);
											} else {
												if (limit > Integer.MAX_VALUE) {
													throw new NumberIsTooLargeException(limit, Integer.MAX_VALUE, true);
												}
												streamSearcher.search(indexSearcher,
														luceneQuery,
														(int) limit,
														luceneSort,
														luceneScoreMode,
														minCompetitiveScore,
														keyFieldName,
														keyScore -> {
															EmitResult result = topKeysSink.tryEmitNext(fixKeyScore(keyScore, scoreDivisor));
															if (result.isFailure() && result != EmitResult.FAIL_CANCELLED && result != EmitResult.FAIL_ZERO_SUBSCRIBER) {
																throw new EmissionException(result);
															}
														},
														totalHitsCount -> {
															EmitResult result = totalHitsCountSink.tryEmitValue(totalHitsCount);
															if (result.isFailure() && result != EmitResult.FAIL_CANCELLED && result != EmitResult.FAIL_ZERO_SUBSCRIBER) {
																throw new EmissionException(result);
															}
														}
												);
											}
											topKeysSink.tryEmitComplete();
										} catch (IOException e) {
											topKeysSink.tryEmitError(e);
											totalHitsCountSink.tryEmitError(e);
										}
									});

									return new LLSearchResult(totalHitsCountSink.asMono(), Flux.just(topKeysSink.asFlux()));
								}).subscribeOn(luceneBlockingScheduler)
						)
						.materialize()
						.flatMap(signal -> {
							if (signal.isOnComplete() || signal.isOnError()) {
								return releaseSearcherWrapper(snapshot, indexSearcher).thenReturn(signal);
							} else {
								return Mono.just(signal);
							}
						})
						.dematerialize()
				);
	}

	@Override
	public Mono<Void> close() {
		return Mono
				.<Void>fromCallable(() -> {
					scheduledTasksLifecycle.cancelAndWait();
					//noinspection BlockingMethodInNonBlockingContext
					indexWriter.close();
					//noinspection BlockingMethodInNonBlockingContext
					directory.close();
					return null;
				})
				.subscribeOn(luceneBlockingScheduler);
	}

	@Override
	public Mono<Void> flush() {
		return Mono
				.<Void>fromCallable(() -> {
					scheduledTasksLifecycle.startScheduledTask();
					try {
						//noinspection BlockingMethodInNonBlockingContext
						indexWriter.commit();
						//noinspection BlockingMethodInNonBlockingContext
						indexWriter.flush();
					} finally {
						scheduledTasksLifecycle.endScheduledTask();
					}
					return null;
				})
				.subscribeOn(luceneBlockingScheduler);
	}

	@Override
	public Mono<Void> refresh() {
		return Mono
				.<Void>fromCallable(() -> {
					scheduledTasksLifecycle.startScheduledTask();
					try {
						//noinspection BlockingMethodInNonBlockingContext
						searcherManager.maybeRefreshBlocking();
					} finally {
						scheduledTasksLifecycle.endScheduledTask();
					}
					return null;
				})
				.subscribeOn(luceneBlockingScheduler);
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
