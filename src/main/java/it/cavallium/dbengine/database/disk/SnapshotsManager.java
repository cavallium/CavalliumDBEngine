package it.cavallium.dbengine.database.disk;

import static it.cavallium.dbengine.client.UninterruptibleScheduler.uninterruptibleScheduler;
import static it.cavallium.dbengine.lucene.LuceneUtils.luceneScheduler;

import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.lucene.LuceneUtils;
import it.cavallium.dbengine.utils.SimpleResource;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.SnapshotDeletionPolicy;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class SnapshotsManager extends SimpleResource {

	private final IndexWriter indexWriter;
	private final SnapshotDeletionPolicy snapshotter;
	private final Phaser activeTasks = new Phaser(1);
	/**
	 * Last snapshot sequence number. 0 is not used
	 */
	private final AtomicLong lastSnapshotSeqNo = new AtomicLong(0);
	/**
	 * LLSnapshot seq no to index commit point
	 */
	private final ConcurrentHashMap<Long, LuceneIndexSnapshot> snapshots = new ConcurrentHashMap<>();

	public SnapshotsManager(IndexWriter indexWriter,
			SnapshotDeletionPolicy snapshotter) {
		this.indexWriter = indexWriter;
		this.snapshotter = snapshotter;
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
		return Mono
				.fromCallable(() -> takeLuceneSnapshot())
				.transform(LuceneUtils::scheduleLucene);
	}

	/**
	 * Use internally. This method commits before taking the snapshot if there are no commits in a new database,
	 * avoiding the exception.
	 */
	private LLSnapshot takeLuceneSnapshot() throws IOException {
		activeTasks.register();
		try {
			if (snapshotter.getSnapshots().isEmpty()) {
				indexWriter.commit();
			}
			var snapshotSeqNo = lastSnapshotSeqNo.incrementAndGet();
			IndexCommit snapshot = snapshotter.snapshot();
			var prevSnapshot = this.snapshots.put(snapshotSeqNo, new LuceneIndexSnapshot(snapshot));

			// Unexpectedly found a snapshot
			if (prevSnapshot != null) {
				try {
					prevSnapshot.close();
				} catch (UncheckedIOException e) {
					throw new IllegalStateException("Can't close snapshot", e);
				}
			}

			return new LLSnapshot(snapshotSeqNo);
		} finally {
			activeTasks.arriveAndDeregister();
		}
	}

	public Mono<Void> releaseSnapshot(LLSnapshot snapshot) {
		return Mono.<Void>fromCallable(() -> {
			activeTasks.register();
			try (var indexSnapshot = this.snapshots.remove(snapshot.getSequenceNumber())) {
				if (indexSnapshot == null) {
					throw new IOException("LLSnapshot " + snapshot.getSequenceNumber() + " not found!");
				}

				var luceneIndexSnapshot = indexSnapshot.getSnapshot();
				snapshotter.release(luceneIndexSnapshot);
				return null;
			} finally {
				activeTasks.arriveAndDeregister();
			}
		}).transform(LuceneUtils::scheduleLucene);
	}

	/**
	 * Returns the total number of snapshots currently held.
	 */
	public int getSnapshotsCount() {
		return Math.max(snapshots.size(), snapshotter.getSnapshotCount());
	}

	@Override
	protected void onClose() {
		if (!activeTasks.isTerminated()) {
			activeTasks.arriveAndAwaitAdvance();
		}
	}
}
