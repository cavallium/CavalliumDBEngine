package it.cavallium.dbengine.database.disk;

import it.cavallium.dbengine.database.LLDocument;
import it.cavallium.dbengine.database.LLLuceneIndex;
import it.cavallium.dbengine.database.LLScoreMode;
import it.cavallium.dbengine.database.LLSearchResult;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.LLSort;
import it.cavallium.dbengine.database.LLTerm;
import it.cavallium.dbengine.database.analyzer.TextFieldsAnalyzer;
import it.cavallium.dbengine.lucene.serializer.Query;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import org.jetbrains.annotations.Nullable;
import org.warp.commonutils.batch.ParallelUtils;
import org.warp.commonutils.functional.IOBiConsumer;
import org.warp.commonutils.functional.TriFunction;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

public class LLLocalMultiLuceneIndex implements LLLuceneIndex {

	private final Long2ObjectMap<LLSnapshot[]> registeredSnapshots = new Long2ObjectOpenHashMap<>();
	private final AtomicLong nextSnapshotNumber = new AtomicLong(1);
	private final LLLocalLuceneIndex[] luceneIndices;

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
		return luceneIndices[0].getLuceneIndexName();
	}

	@Override
	public Mono<Void> addDocument(LLTerm id, LLDocument doc) {
		return getLuceneIndex(id).addDocument(id, doc);
	}

	@Override
	public Mono<Void> addDocuments(Flux<GroupedFlux<LLTerm, LLDocument>> documents) {
		return documents.flatMap(docs -> getLuceneIndex(docs.key()).addDocuments(documents)).then();
	}

	private Mono<Void> runPerInstance(Iterable<LLTerm> keys,
			Iterable<LLDocument> documents,
			TriFunction<LLLuceneIndex, Iterable<LLTerm>, Iterable<LLDocument>, Mono<Void>> consumer) {
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

		return Flux
				.fromIterable(perInstanceKeys.int2ObjectEntrySet())
				.flatMap(currentInstanceEntry -> {
					int instanceId = currentInstanceEntry.getIntKey();
					List<LLTerm> currentInstanceKeys = currentInstanceEntry.getValue();
					return consumer.apply(this.luceneIndices[instanceId], currentInstanceKeys, perInstanceDocs.get(instanceId));
				})
				.then();
	}

	@Override
	public Mono<Void> deleteDocument(LLTerm id) {
		return getLuceneIndex(id).deleteDocument(id);
	}

	@Override
	public Mono<Void> updateDocument(LLTerm id, LLDocument document) {
		return getLuceneIndex(id).updateDocument(id, document);
	}

	@Override
	public Mono<Void> updateDocuments(Flux<GroupedFlux<LLTerm, LLDocument>> documents) {
		return documents.flatMap(docs -> getLuceneIndex(docs.key()).updateDocuments(documents)).then();
	}

	@Override
	public Mono<Void> deleteAll() {
		return Flux
				.fromArray(luceneIndices)
				.flatMap(LLLocalLuceneIndex::deleteAll)
				.then();
	}

	private LLSnapshot resolveSnapshot(LLSnapshot multiSnapshot, int instanceId) {
		if (multiSnapshot != null) {
			return registeredSnapshots.get(multiSnapshot.getSequenceNumber())[instanceId];
		} else {
			return null;
		}
	}

	private Optional<LLSnapshot> resolveSnapshotOptional(LLSnapshot multiSnapshot, int instanceId) {
		return Optional.ofNullable(resolveSnapshot(multiSnapshot, instanceId));
	}

	@Override
	public Mono<LLSearchResult> moreLikeThis(@Nullable LLSnapshot snapshot,
			Flux<Tuple2<String, Set<String>>> mltDocumentFields,
			int limit,
			String keyFieldName) {
		return Flux
				.fromArray(luceneIndices)
				.index()
				.flatMap(tuple -> Mono
						.fromCallable(() -> resolveSnapshotOptional(snapshot, (int) (long) tuple.getT1()))
						.map(luceneSnapshot -> Tuples.of(tuple.getT2(), luceneSnapshot)))
				.flatMap(tuple -> tuple.getT1().moreLikeThis(tuple.getT2().orElse(null), mltDocumentFields, limit, keyFieldName))
				.reduce(LLSearchResult.accumulator());
	}

	@Override
	public Mono<LLSearchResult> search(@Nullable LLSnapshot snapshot,
			Query query,
			int limit,
			@Nullable LLSort sort,
			LLScoreMode scoreMode,
			String keyFieldName) {
		return Flux
				.fromArray(luceneIndices)
				.index()
				.flatMap(tuple -> Mono
						.fromCallable(() -> resolveSnapshotOptional(snapshot, (int) (long) tuple.getT1()))
						.map(luceneSnapshot -> Tuples.of(tuple.getT2(), luceneSnapshot)))
				.flatMap(tuple -> tuple.getT1().search(tuple.getT2().orElse(null), query, limit, sort, scoreMode, keyFieldName))
				.reduce(LLSearchResult.accumulator());
	}

	@Override
	public Mono<Void> close() {
		return Flux
				.fromArray(luceneIndices)
				.flatMap(LLLocalLuceneIndex::close)
				.then();
	}

	@Override
	public Mono<Void> flush() {
		return Flux
				.fromArray(luceneIndices)
				.flatMap(LLLocalLuceneIndex::flush)
				.then();
	}

	@Override
	public Mono<Void> refresh() {
		return Flux
				.fromArray(luceneIndices)
				.flatMap(LLLocalLuceneIndex::refresh)
				.then();
	}

	@Override
	public Mono<LLSnapshot> takeSnapshot() {
		return Mono
				.fromCallable(() -> {
					CopyOnWriteArrayList<LLSnapshot> instancesSnapshots
							= new CopyOnWriteArrayList<>(new LLSnapshot[luceneIndices.length]);
					var snapIndex = nextSnapshotNumber.getAndIncrement();

					ParallelUtils.parallelizeIO((IOBiConsumer<LLLuceneIndex, Integer> s) -> {
						for (int i = 0; i < luceneIndices.length; i++) {
							s.consume(luceneIndices[i], i);
						}
					}, maxQueueSize, luceneIndices.length, 1, (instance, i) -> {
						var instanceSnapshot = instance.takeSnapshot();
						//todo: reimplement better (don't block and take them parallel)
						instancesSnapshots.set(i, instanceSnapshot.block());
					});

					LLSnapshot[] instancesSnapshotsArray = instancesSnapshots.toArray(LLSnapshot[]::new);
					registeredSnapshots.put(snapIndex, instancesSnapshotsArray);

					return new LLSnapshot(snapIndex);
				})
				.subscribeOn(Schedulers.boundedElastic());
	}

	@Override
	public Mono<Void> releaseSnapshot(LLSnapshot snapshot) {
		return Mono
				.<Void>fromCallable(() -> {
					LLSnapshot[] instancesSnapshots = registeredSnapshots.remove(snapshot.getSequenceNumber());
					for (int i = 0; i < luceneIndices.length; i++) {
						LLLocalLuceneIndex luceneIndex = luceneIndices[i];
						//todo: reimplement better (don't block and take them parallel)
						luceneIndex.releaseSnapshot(instancesSnapshots[i]).block();
					}
					return null;
				})
				.subscribeOn(Schedulers.boundedElastic());
	}

	@Override
	public boolean isLowMemoryMode() {
		return luceneIndices[0].isLowMemoryMode();
	}
}
