package it.cavallium.dbengine.database.disk;

import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.lucene.ScheduledTaskLifecycle;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.SnapshotDeletionPolicy;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class SnapshotsManager {

	private final IndexWriter indexWriter;
	private final SnapshotDeletionPolicy snapshotter;
	private final ScheduledTaskLifecycle scheduledTasksLifecycle;
	/**
	 * Last snapshot sequence number. 0 is not used
	 */
	private final AtomicLong lastSnapshotSeqNo = new AtomicLong(0);
	/**
	 * LLSnapshot seq no to index commit point
	 */
	private final ConcurrentHashMap<Long, LuceneIndexSnapshot> snapshots = new ConcurrentHashMap<>();

	public SnapshotsManager(IndexWriter indexWriter,
			SnapshotDeletionPolicy snapshotter,
			ScheduledTaskLifecycle scheduledTasksLifecycle) {
		this.indexWriter = indexWriter;
		this.snapshotter = snapshotter;
		this.scheduledTasksLifecycle = scheduledTasksLifecycle;
	}

	public LuceneIndexSnapshot resolveSnapshot(@Nullable LLSnapshot snapshot) {
		if (snapshot == null) {
			return null;
		}
		return Objects.requireNonNull(snapshots.get(snapshot.getSequenceNumber()),
				() -> "Can't resolve snapshot " + snapshot.getSequenceNumber()
		);
	}

	public Mono<LLSnapshot> takeSnapshot() {
		return takeLuceneSnapshot().map(snapshot -> {
			var snapshotSeqNo = lastSnapshotSeqNo.incrementAndGet();
			this.snapshots.put(snapshotSeqNo, new LuceneIndexSnapshot(snapshot));
			return new LLSnapshot(snapshotSeqNo);
		});
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
										indexWriter.commit();
										return snapshotter.snapshot();
									} finally {
										scheduledTasksLifecycle.endScheduledTask();
									}
								});
							} else {
								return Mono.error(ex);
							}
						})
				);
	}

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
}
