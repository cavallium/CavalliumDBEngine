package it.cavallium.dbengine.database.disk;

import com.google.common.base.Suppliers;
import it.cavallium.dbengine.client.query.QueryParser;
import it.cavallium.dbengine.client.query.current.data.QueryParams;
import it.cavallium.dbengine.database.EnglishItalianStopFilter;
import it.cavallium.dbengine.database.LLDocument;
import it.cavallium.dbengine.database.LLKeyScore;
import it.cavallium.dbengine.database.LLLuceneIndex;
import it.cavallium.dbengine.database.LLSearchCollectionStatisticsGetter;
import it.cavallium.dbengine.database.LLSearchResult;
import it.cavallium.dbengine.database.LLSignal;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.LLTerm;
import it.cavallium.dbengine.database.LLTotalHitsCount;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.lucene.LuceneUtils;
import it.cavallium.dbengine.lucene.ScheduledTaskLifecycle;
import it.cavallium.dbengine.lucene.analyzer.TextFieldsAnalyzer;
import it.cavallium.dbengine.lucene.analyzer.TextFieldsSimilarity;
import it.cavallium.dbengine.lucene.searcher.AdaptiveStreamSearcher;
import it.cavallium.dbengine.lucene.searcher.AllowOnlyQueryParsingCollectorStreamSearcher;
import it.cavallium.dbengine.lucene.searcher.LuceneStreamSearcher;
import it.cavallium.dbengine.lucene.searcher.LuceneStreamSearcher.HandleResult;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.SnapshotDeletionPolicy;
import org.apache.lucene.queries.mlt.MoreLikeThis;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.similarities.TFIDFSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.jetbrains.annotations.Nullable;
import org.warp.commonutils.log.Logger;
import org.warp.commonutils.log.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink.OverflowStrategy;
import reactor.core.publisher.GroupedFlux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

public class LLLocalLuceneIndex implements LLLuceneIndex {

	protected static final Logger logger = LoggerFactory.getLogger(LLLocalLuceneIndex.class);
	private static final LuceneStreamSearcher streamSearcher = new AdaptiveStreamSearcher();
	private static final AllowOnlyQueryParsingCollectorStreamSearcher allowOnlyQueryParsingCollectorStreamSearcher = new AllowOnlyQueryParsingCollectorStreamSearcher();
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
	private final Scheduler luceneBlockingScheduler;
	private static final Function<String, Scheduler> boundedSchedulerSupplier = name -> Schedulers.newBoundedElastic(Schedulers.DEFAULT_BOUNDED_ELASTIC_SIZE,
			Schedulers.DEFAULT_BOUNDED_ELASTIC_QUEUESIZE,
			"lucene-" + name,
			60
	);
	private final Supplier<Scheduler> lowMemorySchedulerSupplier = Suppliers.memoize(() ->
			Schedulers.newBoundedElastic(1, Schedulers.DEFAULT_BOUNDED_ELASTIC_QUEUESIZE, "lucene-low-memory", Integer.MAX_VALUE))::get;
	private final Supplier<Scheduler> querySchedulerSupplier = Suppliers.memoize(() -> boundedSchedulerSupplier.apply("query"))::get;
	private final Supplier<Scheduler> blockingSchedulerSupplier = Suppliers.memoize(() -> boundedSchedulerSupplier.apply("blocking"))::get;
	private final Supplier<Scheduler> blockingLuceneSearchSchedulerSupplier = Suppliers.memoize(() -> boundedSchedulerSupplier.apply("search-blocking"))::get;
	/**
	 * Lucene query scheduler.
	 */
	private final Scheduler luceneQueryScheduler;
	private final Scheduler blockingLuceneSearchScheduler;

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
		if (lowMemory) {
			this.luceneQueryScheduler = this.luceneBlockingScheduler = lowMemorySchedulerSupplier.get();
		} else {
			this.luceneBlockingScheduler = blockingSchedulerSupplier.get();
			this.luceneQueryScheduler = querySchedulerSupplier.get();
		}
		this.blockingLuceneSearchScheduler = blockingLuceneSearchSchedulerSupplier.get();

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
		scheduledTasksLifecycle.registerScheduledTask(luceneHeavyTasksScheduler.schedulePeriodically(() -> {
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
		return takeLuceneSnapshot()
						.flatMap(snapshot -> Mono
								.fromCallable(() -> {
									var snapshotSeqNo = lastSnapshotSeqNo.incrementAndGet();
									this.snapshots.put(snapshotSeqNo, new LuceneIndexSnapshot(snapshot));
									return new LLSnapshot(snapshotSeqNo);
								})
								.subscribeOn(luceneBlockingScheduler)
						);
	}

	/**
	 * Use internally. This method commits before taking the snapshot if there are no commits in a new database,
	 * avoiding the exception.
	 */
	private Mono<IndexCommit> takeLuceneSnapshot() {
		return Mono
				.fromCallable(snapshotter::snapshot)
				.subscribeOn(luceneBlockingScheduler)
				.onErrorResume(ex -> Mono
						.defer(() -> {
							if (ex instanceof IllegalStateException && "No index commit to snapshot".equals(ex.getMessage())) {
								return Mono.fromCallable(() -> {
									//noinspection BlockingMethodInNonBlockingContext
									indexWriter.commit();
									//noinspection BlockingMethodInNonBlockingContext
									return snapshotter.snapshot();
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
			var indexSnapshot = this.snapshots.remove(snapshot.getSequenceNumber());
			if (indexSnapshot == null) {
				throw new IOException("LLSnapshot " + snapshot.getSequenceNumber() + " not found!");
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
			indexWriter.forceMergeDeletes(true);
			//noinspection BlockingMethodInNonBlockingContext
			indexWriter.commit();
			return null;
		}).subscribeOn(luceneHeavyTasksScheduler);
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
			QueryParams queryParams,
			String keyFieldName,
			Flux<Tuple2<String, Set<String>>> mltDocumentFieldsFlux) {
		return moreLikeThis(snapshot,
				queryParams,
				keyFieldName,
				mltDocumentFieldsFlux,
				false,
				0,
				1
		);
	}

	public Mono<LLSearchResult> distributedMoreLikeThis(@Nullable LLSnapshot snapshot,
			QueryParams queryParams,
			String keyFieldName,
			Flux<Tuple2<String, Set<String>>> mltDocumentFieldsFlux,
			long actionId,
			int scoreDivisor) {
		return moreLikeThis(snapshot,
				queryParams,
				keyFieldName,
				mltDocumentFieldsFlux,
				false,
				actionId,
				scoreDivisor
		);
	}

	public Mono<Void> distributedPreMoreLikeThis(@Nullable LLSnapshot snapshot,
			QueryParams queryParams,
			String keyFieldName,
			Flux<Tuple2<String, Set<String>>> mltDocumentFieldsFlux,
			 long actionId) {
		return moreLikeThis(snapshot,
				queryParams,
				keyFieldName,
				mltDocumentFieldsFlux,
				true,
				actionId,
				1
		)
				.flatMap(LLSearchResult::completion);
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	private Mono<LLSearchResult> moreLikeThis(@Nullable LLSnapshot snapshot,
			QueryParams queryParams,
			String keyFieldName,
			Flux<Tuple2<String, Set<String>>> mltDocumentFieldsFlux,
			boolean doDistributedPre,
			long actionId,
			int scoreDivisor) {
		Query luceneAdditionalQuery;
		try {
			luceneAdditionalQuery = QueryParser.toQuery(queryParams.getQuery());
		} catch (Exception e) {
			return Mono.error(e);
		}
		return mltDocumentFieldsFlux
				.collectMap(Tuple2::getT1, Tuple2::getT2, HashMap::new)
				.flatMap(mltDocumentFields -> {
					mltDocumentFields.entrySet().removeIf(entry -> entry.getValue().isEmpty());
					if (mltDocumentFields.isEmpty()) {
						return Mono.just(LLSearchResult.empty());
					}

					return acquireSearcherWrapper(snapshot, doDistributedPre, actionId).flatMap(indexSearcher -> Mono
							.fromCallable(() -> {
								var mlt = new MoreLikeThis(indexSearcher.getIndexReader());
								mlt.setAnalyzer(indexWriter.getAnalyzer());
								mlt.setFieldNames(mltDocumentFields.keySet().toArray(String[]::new));
								mlt.setMinTermFreq(1);
								mlt.setMinDocFreq(3);
								mlt.setMaxDocFreqPct(20);
								mlt.setBoost(QueryParser.isScoringEnabled(queryParams));
								mlt.setStopWords(EnglishItalianStopFilter.getStopWordsString());
								var similarity = getSimilarity();
								if (similarity instanceof TFIDFSimilarity) {
									mlt.setSimilarity((TFIDFSimilarity) similarity);
								} else {
									logger.trace("Using an unsupported similarity algorithm for MoreLikeThis: {}. You must use a similarity instance based on TFIDFSimilarity!", similarity);
								}

								// Get the reference doc and apply it to MoreLikeThis, to generate the query
								//noinspection BlockingMethodInNonBlockingContext
								var mltQuery = mlt.like((Map) mltDocumentFields);
								Query luceneQuery;
								if (luceneAdditionalQuery != null) {
									luceneQuery = new BooleanQuery.Builder()
											.add(mltQuery, Occur.MUST)
											.add(new ConstantScoreQuery(luceneAdditionalQuery), Occur.MUST)
											.build();
								} else {
									luceneQuery = mltQuery;
								}

								return luceneQuery;
							})
							.subscribeOn(luceneQueryScheduler)
							.map(luceneQuery -> luceneSearch(doDistributedPre,
									indexSearcher,
									queryParams.getLimit(),
									queryParams.getMinCompetitiveScore().getNullable(),
									keyFieldName,
									scoreDivisor,
									luceneQuery,
									QueryParser.toSort(queryParams.getSort()),
									QueryParser.toScoreMode(queryParams.getScoreMode())
							))
							.materialize()
							.flatMap(signal -> {
								if (signal.isOnComplete() || signal.isOnError()) {
									return releaseSearcherWrapper(snapshot, indexSearcher).thenReturn(signal);
								} else {
									return Mono.just(signal);
								}
							}).dematerialize());
				});
	}

	private LLKeyScore fixKeyScore(LLKeyScore keyScore, int scoreDivisor) {
		return scoreDivisor == 1 ? keyScore : new LLKeyScore(keyScore.getKey(), keyScore.getScore() / (float) scoreDivisor);
	}

	@Override
	public Mono<LLSearchResult> search(@Nullable LLSnapshot snapshot, QueryParams queryParams, String keyFieldName) {
		return search(snapshot, queryParams, keyFieldName, false, 0, 1);
	}

	public Mono<LLSearchResult> distributedSearch(@Nullable LLSnapshot snapshot, QueryParams queryParams, String keyFieldName, long actionId, int scoreDivisor) {
		return search(snapshot, queryParams, keyFieldName, false, actionId, scoreDivisor);
	}

	public Mono<Void> distributedPreSearch(@Nullable LLSnapshot snapshot, QueryParams queryParams, String keyFieldName, long actionId) {
		return this
				.search(snapshot, queryParams, keyFieldName, true, actionId, 1)
				.flatMap(LLSearchResult::completion);
	}

	private Mono<LLSearchResult> search(@Nullable LLSnapshot snapshot,
			QueryParams queryParams, String keyFieldName,
			boolean doDistributedPre, long actionId, int scoreDivisor) {
		return acquireSearcherWrapper(snapshot, doDistributedPre, actionId)
				.flatMap(indexSearcher -> Mono
						.fromCallable(() -> {
							Objects.requireNonNull(queryParams.getScoreMode(), "ScoreMode must not be null");
							Query luceneQuery = QueryParser.toQuery(queryParams.getQuery());
							Sort luceneSort = QueryParser.toSort(queryParams.getSort());
							org.apache.lucene.search.ScoreMode luceneScoreMode = QueryParser.toScoreMode(queryParams.getScoreMode());
							return Tuples.of(luceneQuery, Optional.ofNullable(luceneSort), luceneScoreMode);
						})
						.subscribeOn(luceneQueryScheduler)
						.flatMap(tuple -> Mono
								.fromSupplier(() -> {
									Query luceneQuery = tuple.getT1();
									Sort luceneSort = tuple.getT2().orElse(null);
									ScoreMode luceneScoreMode = tuple.getT3();

									return luceneSearch(doDistributedPre,
											indexSearcher,
											queryParams.getLimit(),
											queryParams.getMinCompetitiveScore().getNullable(),
											keyFieldName,
											scoreDivisor,
											luceneQuery,
											luceneSort,
											luceneScoreMode
									);
								})
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

	private LLSearchResult luceneSearch(boolean doDistributedPre,
			IndexSearcher indexSearcher,
			long limit,
			@Nullable Float minCompetitiveScore,
			String keyFieldName,
			int scoreDivisor,
			Query luceneQuery,
			Sort luceneSort,
			ScoreMode luceneScoreMode) {
		return new LLSearchResult(Flux.just(Flux.defer(() -> Flux.<LLSignal>create(sink -> {
			AtomicBoolean cancelled = new AtomicBoolean();
			AtomicLong requests = new AtomicLong();
			Semaphore requestsAvailable = new Semaphore(0);
			sink.onDispose(() -> {
				cancelled.set(true);
				requestsAvailable.release();
			});
			sink.onCancel(() -> {
				cancelled.set(true);
				requestsAvailable.release();
			});
			sink.onRequest(delta -> {
				requests.addAndGet(delta);
				requestsAvailable.release();
			});

			try {
				if (!cancelled.get()) {
					if (doDistributedPre) {
						//noinspection BlockingMethodInNonBlockingContext
						allowOnlyQueryParsingCollectorStreamSearcher.search(indexSearcher, luceneQuery);
						sink.next(new LLTotalHitsCount(0L));
					} else {
						int boundedLimit = Math.max(0, limit > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) limit);
						//noinspection BlockingMethodInNonBlockingContext
						streamSearcher.search(indexSearcher,
								luceneQuery,
								boundedLimit,
								luceneSort,
								luceneScoreMode,
								minCompetitiveScore,
								keyFieldName,
								keyScore -> {
									try {
										if (cancelled.get()) {
											return HandleResult.HALT;
										}
										while (requests.decrementAndGet() < 0) {
											requests.incrementAndGet();
											requestsAvailable.acquire();
											if (cancelled.get()) {
												return HandleResult.HALT;
											}
										}
										sink.next(fixKeyScore(keyScore, scoreDivisor));
										return HandleResult.CONTINUE;
									} catch (Exception ex) {
										sink.error(ex);
										cancelled.set(true);
										requestsAvailable.release();
										return HandleResult.HALT;
									}
								},
								totalHitsCount -> {
									try {
										if (cancelled.get()) {
											return;
										}
										while (requests.decrementAndGet() < 0) {
											requests.incrementAndGet();
											requestsAvailable.acquire();
											if (cancelled.get()) {
												return;
											}
										}
										sink.next(new LLTotalHitsCount(totalHitsCount));
									} catch (Exception ex) {
										sink.error(ex);
										cancelled.set(true);
										requestsAvailable.release();
									}
								}
						);
					}
					sink.complete();
				}
			} catch (Exception ex) {
				sink.error(ex);
			}
		}, OverflowStrategy.ERROR).subscribeOn(blockingLuceneSearchScheduler).publishOn(luceneQueryScheduler))));
	}

	@Override
	public Mono<Void> close() {
		return Mono
				.<Void>fromCallable(() -> {
					this.blockingLuceneSearchScheduler.dispose();
					scheduledTasksLifecycle.cancelAndWait();
					//noinspection BlockingMethodInNonBlockingContext
					indexWriter.close();
					//noinspection BlockingMethodInNonBlockingContext
					directory.close();
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
