package it.cavallium.dbengine.database.disk;

import it.cavallium.dbengine.database.LLDocument;
import it.cavallium.dbengine.database.LLKeyScore;
import it.cavallium.dbengine.database.LLLuceneIndex;
import it.cavallium.dbengine.database.LLScoreMode;
import it.cavallium.dbengine.database.LLSearchResult;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.LLSort;
import it.cavallium.dbengine.database.LLTerm;
import it.cavallium.dbengine.database.LLTopKeys;
import it.cavallium.dbengine.database.analyzer.TextFieldsAnalyzer;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.StampedLock;
import org.jetbrains.annotations.Nullable;
import org.warp.commonutils.batch.ParallelUtils;
import org.warp.commonutils.functional.IOBiConsumer;
import org.warp.commonutils.functional.IOConsumer;
import org.warp.commonutils.functional.IOTriConsumer;
import org.warp.commonutils.locks.LockUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.function.Tuples;

public class LLLocalMultiLuceneIndex implements LLLuceneIndex {

	private final Long2ObjectMap<LLSnapshot[]> registeredSnapshots = new Long2ObjectOpenHashMap<>();
	private final AtomicLong nextSnapshotNumber = new AtomicLong(1);
	private final LLLocalLuceneIndex[] luceneIndices;
	private final StampedLock access = new StampedLock();

	private final int maxQueueSize = 1000;

	public LLLocalMultiLuceneIndex(Path lucene,
			String name,
			int instancesCount,
			TextFieldsAnalyzer textFieldsAnalyzer,
			Duration queryRefreshDebounceTime,
			Duration commitDebounceTime,
			boolean lowMemory) throws IOException {

		if (instancesCount <= 1 || instancesCount > 100) {
			throw new IOException("Unsupported instances count: " + instancesCount);
		}

		LLLocalLuceneIndex[] luceneIndices = new LLLocalLuceneIndex[instancesCount];
		for (int i = 0; i < instancesCount; i++) {
			String instanceName;
			if (i == 0) {
				instanceName = name;
			} else {
				instanceName = name + "_" + String.format("%03d", i);
			}
			luceneIndices[i] = new LLLocalLuceneIndex(lucene,
					instanceName,
					textFieldsAnalyzer,
					queryRefreshDebounceTime,
					commitDebounceTime,
					lowMemory
			);
		}
		this.luceneIndices = luceneIndices;
	}

	private LLLocalLuceneIndex getLuceneIndex(LLTerm id) {
		return luceneIndices[getLuceneIndexId(id)];
	}

	private int getLuceneIndexId(LLTerm id) {
		return Math.abs(id.getValue().hashCode()) % luceneIndices.length;
	}

	@Override
	public String getLuceneIndexName() {
		return LockUtils.readLock(access, () -> luceneIndices[0].getLuceneIndexName());
	}

	@Override
	public void addDocument(LLTerm id, LLDocument doc) throws IOException {
		LockUtils.readLockIO(access, () -> getLuceneIndex(id).addDocument(id, doc));
	}

	@Override
	public void addDocuments(Iterable<LLTerm> keys, Iterable<LLDocument> documents) throws IOException {
		LockUtils.readLockIO(access, () -> {
			ParallelUtils.parallelizeIO(s -> runPerInstance(keys, documents, s),
					maxQueueSize,
					luceneIndices.length,
					1,
					LLLuceneIndex::addDocuments
			);
		});
	}

	private void runPerInstance(Iterable<LLTerm> keys,
			Iterable<LLDocument> documents,
			IOTriConsumer<LLLuceneIndex, Iterable<LLTerm>, Iterable<LLDocument>> consumer) throws IOException {
		var keysIt = keys.iterator();
		var docsIt = documents.iterator();

		Int2ObjectMap<List<LLTerm>> perInstanceKeys = new Int2ObjectOpenHashMap<>();
		Int2ObjectMap<List<LLDocument>> perInstanceDocs = new Int2ObjectOpenHashMap<>();

		while (keysIt.hasNext()) {
			LLTerm key = keysIt.next();
			LLDocument doc = docsIt.next();
			var instanceId = getLuceneIndexId(key);

			perInstanceKeys.computeIfAbsent(instanceId, iid -> new ArrayList<>()).add(key);
			perInstanceDocs.computeIfAbsent(instanceId, iid -> new ArrayList<>()).add(doc);
		}

		for (Int2ObjectMap.Entry<List<LLTerm>> currentInstanceEntry : perInstanceKeys.int2ObjectEntrySet()) {
			int instanceId = currentInstanceEntry.getIntKey();
			List<LLTerm> currentInstanceKeys = currentInstanceEntry.getValue();
			consumer.accept(this.luceneIndices[instanceId], currentInstanceKeys, perInstanceDocs.get(instanceId));
		}
	}

	@Override
	public void deleteDocument(LLTerm id) throws IOException {
		LockUtils.readLockIO(access, () -> getLuceneIndex(id).deleteDocument(id));
	}

	@Override
	public void updateDocument(LLTerm id, LLDocument document) throws IOException {
		LockUtils.readLockIO(access, () -> getLuceneIndex(id).updateDocument(id, document));
	}

	@Override
	public void updateDocuments(Iterable<LLTerm> keys, Iterable<LLDocument> documents) throws IOException {
		LockUtils.readLockIO(access, () -> {
			ParallelUtils.parallelizeIO(s -> runPerInstance(keys, documents, s),
					maxQueueSize,
					luceneIndices.length,
					1,
					LLLuceneIndex::updateDocuments
			);
		});
	}

	@Override
	public void deleteAll() throws IOException {
		LockUtils.writeLockIO(access, () -> {
			ParallelUtils.parallelizeIO((IOConsumer<LLLuceneIndex> s) -> {
				for (LLLocalLuceneIndex luceneIndex : luceneIndices) {
					s.consume(luceneIndex);
				}
			}, maxQueueSize, luceneIndices.length, 1, LLLuceneIndex::deleteAll);
		});
	}

	private LLTopKeys mergeTopKeys(Collection<LLTopKeys> multi) {
		long totalHitsCount = 0;
		LLKeyScore[] hits;
		int hitsArraySize = 0;
		for (LLTopKeys llTopKeys : multi) {
			totalHitsCount += llTopKeys.getTotalHitsCount();
			hitsArraySize += llTopKeys.getHits().length;
		}
		hits = new LLKeyScore[hitsArraySize];

		int offset = 0;
		for (LLTopKeys llTopKeys : multi) {
			var part = llTopKeys.getHits();
			System.arraycopy(part, 0, hits, offset, part.length);
			offset += part.length;
		}

		return new LLTopKeys(totalHitsCount, hits);
	}

	private LLSnapshot resolveSnapshot(LLSnapshot multiSnapshot, int instanceId) {
		if (multiSnapshot != null) {
			return registeredSnapshots.get(multiSnapshot.getSequenceNumber())[instanceId];
		} else {
			return null;
		}
	}

	@Override
	public Mono<LLSearchResult> moreLikeThis(@Nullable LLSnapshot snapshot,
			Map<String, Set<String>> mltDocumentFields,
			int limit,
			String keyFieldName) {
		return Mono
				.fromSupplier(access::readLock)
				.subscribeOn(Schedulers.boundedElastic())
				.flatMap(stamp -> Flux
						.fromArray(luceneIndices)
						.index()
						.flatMap(tuple -> Mono
								.fromCallable(() -> resolveSnapshot(snapshot, (int) (long) tuple.getT1()))
								.subscribeOn(Schedulers.boundedElastic())
								.map(luceneSnapshot -> Tuples.of(tuple.getT2(), luceneSnapshot))
						)
						.flatMap(tuple -> tuple.getT1().moreLikeThis(tuple.getT2(), mltDocumentFields, limit, keyFieldName))
						.reduce(LLSearchResult.accumulator())
						.materialize()
						.flatMap(signal -> Mono
								.fromRunnable(() -> access.unlockRead(stamp))
								.subscribeOn(Schedulers.boundedElastic())
								.thenReturn(signal)
						)
						.dematerialize()
				);
	}

	@Override
	public Mono<LLSearchResult> search(@Nullable LLSnapshot snapshot,
			String query,
			int limit,
			@Nullable LLSort sort,
			LLScoreMode scoreMode,
			String keyFieldName) {
		return Mono
				.fromSupplier(access::readLock)
				.subscribeOn(Schedulers.boundedElastic())
				.flatMap(stamp -> Flux
						.fromArray(luceneIndices)
						.index()
						.flatMap(tuple -> Mono
										.fromCallable(() -> resolveSnapshot(snapshot, (int) (long) tuple.getT1()))
										.subscribeOn(Schedulers.boundedElastic())
										.map(luceneSnapshot -> Tuples.of(tuple.getT2(), luceneSnapshot))
						)
						.flatMap(tuple -> tuple.getT1().search(tuple.getT2(), query, limit, sort, scoreMode, keyFieldName))
						.reduce(LLSearchResult.accumulator())
						.materialize()
						.flatMap(signal -> Mono
								.fromRunnable(() -> access.unlockRead(stamp))
								.subscribeOn(Schedulers.boundedElastic())
								.thenReturn(signal)
						)
						.dematerialize()
				);
	}

	@Override
	public long count(@Nullable LLSnapshot snapshot, String query) throws IOException {
		return LockUtils.readLockIO(access, () -> {
			AtomicLong result = new AtomicLong(0);

			ParallelUtils.parallelizeIO((IOBiConsumer<LLLuceneIndex, LLSnapshot> s) -> {
				for (int i = 0; i < luceneIndices.length; i++) {
					s.consume(luceneIndices[i], resolveSnapshot(snapshot, i));
				}
			}, maxQueueSize, luceneIndices.length, 1, (instance, instanceSnapshot) -> {
				result.addAndGet(instance.count(instanceSnapshot, query));
			});
			return result.get();
		});
	}

	@Override
	public void close() throws IOException {
		LockUtils.writeLockIO(access, () -> {
			ParallelUtils.parallelizeIO((IOConsumer<LLLuceneIndex> s) -> {
				for (LLLocalLuceneIndex luceneIndex : luceneIndices) {
					s.consume(luceneIndex);
				}
			}, maxQueueSize, luceneIndices.length, 1, Closeable::close);
		});
	}

	@Override
	public LLSnapshot takeSnapshot() throws IOException {
		return LockUtils.writeLockIO(access, () -> {
			CopyOnWriteArrayList<LLSnapshot> instancesSnapshots = new CopyOnWriteArrayList<>(new LLSnapshot[luceneIndices.length]);
			var snapIndex = nextSnapshotNumber.getAndIncrement();

			ParallelUtils.parallelizeIO((IOBiConsumer<LLLuceneIndex, Integer> s) -> {
				for (int i = 0; i < luceneIndices.length; i++) {
					s.consume(luceneIndices[i], i);
				}
			}, maxQueueSize, luceneIndices.length, 1, (instance, i) -> {
				var instanceSnapshot = instance.takeSnapshot();
				instancesSnapshots.set(i, instanceSnapshot);
			});

			LLSnapshot[] instancesSnapshotsArray = instancesSnapshots.toArray(LLSnapshot[]::new);
			registeredSnapshots.put(snapIndex, instancesSnapshotsArray);

			return new LLSnapshot(snapIndex);
		});
	}

	@Override
	public void releaseSnapshot(LLSnapshot snapshot) throws IOException {
		LockUtils.writeLockIO(access, () -> {
			LLSnapshot[] instancesSnapshots = registeredSnapshots.remove(snapshot.getSequenceNumber());
			for (int i = 0; i < luceneIndices.length; i++) {
				LLLocalLuceneIndex luceneIndex = luceneIndices[i];
				luceneIndex.releaseSnapshot(instancesSnapshots[i]);
			}
		});
	}

	@Override
	public boolean isLowMemoryMode() {
		return luceneIndices[0].isLowMemoryMode();
	}
}
