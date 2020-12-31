package it.cavallium.dbengine.database.disk;

import it.cavallium.dbengine.database.LLDocument;
import it.cavallium.dbengine.database.LLKeyScore;
import it.cavallium.dbengine.database.LLLuceneIndex;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.LLSort;
import it.cavallium.dbengine.database.LLTerm;
import it.cavallium.dbengine.database.LLTopKeys;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.LuceneUtils;
import it.cavallium.dbengine.database.analyzer.TextFieldsAnalyzer;
import it.cavallium.dbengine.database.luceneutil.AdaptiveStreamSearcher;
import it.cavallium.dbengine.database.luceneutil.LuceneStreamSearcher;
import it.cavallium.luceneserializer.luceneserializer.ParseException;
import it.cavallium.luceneserializer.luceneserializer.QueryParser;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.SnapshotDeletionPolicy;
import org.apache.lucene.queries.mlt.MoreLikeThis;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSLockFactory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.util.Constants;
import org.jetbrains.annotations.Nullable;
import org.warp.commonutils.concurrency.executor.ScheduledTaskLifecycle;
import org.warp.commonutils.functional.IOFunction;
import org.warp.commonutils.type.ShortNamedThreadFactory;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
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
		if (Constants.WINDOWS) {
			//noinspection deprecation
			this.directory = new SimpleFSDirectory(directoryPath, FSLockFactory.getDefault());
		} else {
			this.directory = new NIOFSDirectory(directoryPath, FSLockFactory.getDefault());
		}
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
		scheduledTasksLifecycle.registerScheduledTask(scheduler.scheduleAtFixedRate(task,
				duration.toMillis(),
				duration.toMillis(),
				TimeUnit.MILLISECONDS
		));
	}

	@Override
	public String getLuceneIndexName() {
		return luceneIndexName;
	}

	@Override
	public LLSnapshot takeSnapshot() throws IOException {

		long snapshotSeqNo = lastSnapshotSeqNo.incrementAndGet();

		IndexCommit snapshot = takeLuceneSnapshot();
		this.snapshots.put(snapshotSeqNo, new LuceneIndexSnapshot(snapshot));
		return new LLSnapshot(snapshotSeqNo);
	}

	/**
	 * Use internally. This method commits before taking the snapshot if there are no commits in a new database,
	 * avoiding the exception.
	 */
	private IndexCommit takeLuceneSnapshot() throws IOException {
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
	}

	@Override
	public void releaseSnapshot(LLSnapshot snapshot) throws IOException {
		var indexSnapshot = this.snapshots.remove(snapshot.getSequenceNumber());
		if (indexSnapshot == null) {
			throw new IOException("Snapshot " + snapshot.getSequenceNumber() + " not found!");
		}

		indexSnapshot.close();

		var luceneIndexSnapshot = indexSnapshot.getSnapshot();
		snapshotter.release(luceneIndexSnapshot);
		// Delete unused files after releasing the snapshot
		indexWriter.deleteUnusedFiles();
	}

	@Override
	public void addDocument(LLTerm key, LLDocument doc) throws IOException {
		indexWriter.addDocument(LLUtils.toDocument(doc));
	}

	@Override
	public void addDocuments(Iterable<LLTerm> keys, Iterable<LLDocument> docs) throws IOException {
		indexWriter.addDocuments(LLUtils.toDocuments(docs));
	}

	@Override
	public void deleteDocument(LLTerm id) throws IOException {
		indexWriter.deleteDocuments(LLUtils.toTerm(id));
	}

	@Override
	public void updateDocument(LLTerm id, LLDocument document) throws IOException {
		indexWriter.updateDocument(LLUtils.toTerm(id), LLUtils.toDocument(document));
	}

	@Override
	public void updateDocuments(Iterable<LLTerm> ids, Iterable<LLDocument> documents)
			throws IOException {
		var idIt = ids.iterator();
		var docIt = documents.iterator();
		while (idIt.hasNext()) {
			var id = idIt.next();
			var doc = docIt.next();

			indexWriter.updateDocument(LLUtils.toTerm(id), LLUtils.toDocument(doc));
		}
	}

	@Override
	public void deleteAll() throws IOException {
		indexWriter.deleteAll();
		indexWriter.commit();
		indexWriter.forceMergeDeletes(true);
		indexWriter.flush();
		indexWriter.commit();
	}

	@Override
	public Collection<LLTopKeys> search(@Nullable LLSnapshot snapshot, String queryString, int limit, @Nullable LLSort sort,
			String keyFieldName)
			throws IOException {
		try {
			var luceneIndexSnapshot = resolveSnapshot(snapshot);

			Query query = QueryParser.parse(queryString);
			Sort luceneSort = LLUtils.toSort(sort);

			return Collections.singleton(runSearch(luceneIndexSnapshot, (indexSearcher) -> {
				return blockingSearch(indexSearcher, limit, query, luceneSort, keyFieldName);
			}));
		} catch (ParseException e) {
			throw new IOException("Error during query count!", e);
		}
	}

	@Override
	public Collection<LLTopKeys> moreLikeThis(@Nullable LLSnapshot snapshot, Map<String, Set<String>> mltDocumentFields, int limit,
			String keyFieldName)
			throws IOException {
		var luceneIndexSnapshot = resolveSnapshot(snapshot);

		if (mltDocumentFields.isEmpty()) {
			return Collections.singleton(new LLTopKeys(0, new LLKeyScore[0]));
		}

		return Collections.singleton(runSearch(luceneIndexSnapshot, (indexSearcher) -> {

			var mlt = new MoreLikeThis(indexSearcher.getIndexReader());
			mlt.setAnalyzer(indexWriter.getAnalyzer());
			mlt.setFieldNames(mltDocumentFields.keySet().toArray(String[]::new));
			mlt.setMinTermFreq(1);
			//mlt.setMinDocFreq(1);
			mlt.setBoost(true);

			// Get the reference doc and apply it to MoreLikeThis, to generate the query
			@SuppressWarnings({"unchecked", "rawtypes"})
			Query query = mlt.like((Map) mltDocumentFields);

			// Search
			return blockingSearch(indexSearcher, limit, query, null, keyFieldName);
		}));
	}

	private static LLTopKeys blockingSearch(IndexSearcher indexSearcher,
			int limit,
			Query query,
			Sort luceneSort,
			String keyFieldName) throws IOException {
		TopDocs results;
		List<LLKeyScore> keyScores;

		results = luceneSort != null ? indexSearcher.search(query, limit, luceneSort)
				: indexSearcher.search(query, limit);
		var hits = ObjectArrayList.wrap(results.scoreDocs);
		keyScores = new LinkedList<>();
		for (ScoreDoc hit : hits) {
			int docId = hit.doc;
			float score = hit.score;
			Document d = indexSearcher.doc(docId, Set.of(keyFieldName));
			if (d.getFields().isEmpty()) {
				System.err.println("The document docId:" + docId + ",score:" + score + " is empty.");
				var realFields = indexSearcher.doc(docId).getFields();
				if (!realFields.isEmpty()) {
					System.err.println("Present fields:");
					for (IndexableField field : realFields) {
						System.err.println(" - " + field.name());
					}
				}
			} else {
				var field = d.getField(keyFieldName);
				if (field == null) {
					System.err.println("Can't get key of document docId:" + docId + ",score:" + score);
				} else {
					keyScores.add(new LLKeyScore(field.stringValue(), score));
				}
			}
		}
		return new LLTopKeys(results.totalHits.value, keyScores.toArray(new LLKeyScore[0]));
	}

	@SuppressWarnings("UnnecessaryLocalVariable")
	@Override
	public Tuple2<Mono<Long>, Collection<Flux<String>>> searchStream(@Nullable LLSnapshot snapshot, String queryString, int limit,
			@Nullable LLSort sort, String keyFieldName) {
		try {
			Query query = QueryParser.parse(queryString);
			Sort luceneSort = LLUtils.toSort(sort);

			var acquireSearcherWrappedBlocking = Mono
					.fromCallable(() -> {
						if (snapshot == null) {
							return searcherManager.acquire();
						} else {
							return resolveSnapshot(snapshot).getIndexSearcher();
						}
					})
					.subscribeOn(Schedulers.boundedElastic());

			EmitterProcessor<Long> countProcessor = EmitterProcessor.create();
			EmitterProcessor<String> resultsProcessor = EmitterProcessor.create();

			var publisher = acquireSearcherWrappedBlocking.flatMapMany(indexSearcher -> {
				return Flux.<Object>push(sink -> {
					try {
						Long approximatedTotalResultsCount = streamSearcher.streamSearch(indexSearcher,
								query,
								limit,
								luceneSort,
								keyFieldName,
								sink::next
						);
						sink.next(approximatedTotalResultsCount);
						sink.complete();
					} catch (IOException e) {
						sink.error(e);
					}
					}).subscribeOn(Schedulers.boundedElastic())
						.doOnTerminate(() -> {
							if (snapshot == null) {
								try {
									searcherManager.release(indexSearcher);
								} catch (IOException e) {
									e.printStackTrace();
								}
							}
						});
			}).publish();

			publisher.filter(item -> item instanceof Long).cast(Long.class).subscribe(countProcessor);
			publisher.filter(item -> item instanceof String).cast(String.class).subscribe(resultsProcessor);

			publisher.connect();

			return Tuples.of(countProcessor.single(0L), Collections.singleton(resultsProcessor));
		} catch (ParseException e) {
			var error = new IOException("Error during query count!", e);
			return Tuples.of(Mono.error(error), Collections.singleton(Flux.error(error)));
		}
	}

	@Override
	public long count(@Nullable LLSnapshot snapshot, String queryString) throws IOException {
		try {
			var luceneIndexSnapshot = resolveSnapshot(snapshot);

			Query query = QueryParser.parse(queryString);

			return (long) runSearch(luceneIndexSnapshot, (indexSearcher) -> indexSearcher.count(query));
		} catch (ParseException e) {
			throw new IOException("Error during query count!", e);
		}
	}

	@Override
	public void close() throws IOException {
		scheduledTasksLifecycle.cancelAndWait();
		indexWriter.close();
		directory.close();
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

	private void scheduledQueryRefresh() {
		try {
			searcherManager.maybeRefreshBlocking();
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}

	private <U> U runSearch(@Nullable LuceneIndexSnapshot snapshot, IOFunction<IndexSearcher, U> searchExecutor)
			throws IOException {
		if (snapshot != null) {
			return searchExecutor.apply(snapshot.getIndexSearcher());
		} else {
			var indexSearcher = searcherManager.acquire();
			try {
				return searchExecutor.apply(indexSearcher);
			} finally {
				searcherManager.release(indexSearcher);
			}
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
