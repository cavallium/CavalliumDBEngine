package it.cavallium.dbengine.database.disk;

import io.net5.buffer.api.Send;
import it.cavallium.dbengine.client.IndicizerAnalyzers;
import it.cavallium.dbengine.client.IndicizerSimilarities;
import it.cavallium.dbengine.client.LuceneOptions;
import it.cavallium.dbengine.client.query.current.data.QueryParams;
import it.cavallium.dbengine.database.LLDocument;
import it.cavallium.dbengine.database.LLLuceneIndex;
import it.cavallium.dbengine.database.LLSearchResultShard;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.LLTerm;
import it.cavallium.dbengine.lucene.LuceneUtils;
import it.cavallium.dbengine.lucene.searcher.AdaptiveLuceneMultiSearcher;
import it.cavallium.dbengine.lucene.searcher.LLSearchTransformer;
import it.cavallium.dbengine.lucene.searcher.LocalQueryParams;
import it.cavallium.dbengine.lucene.searcher.LuceneMultiSearcher;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.util.function.Tuple2;

public class LLLocalMultiLuceneIndex implements LLLuceneIndex {

	// Scheduler used to get callback values of LuceneStreamSearcher without creating deadlocks
	protected final Scheduler luceneSearcherScheduler = LuceneUtils.newLuceneSearcherScheduler(true);

	private final ConcurrentHashMap<Long, LLSnapshot[]> registeredSnapshots = new ConcurrentHashMap<>();
	private final AtomicLong nextSnapshotNumber = new AtomicLong(1);
	private final LLLocalLuceneIndex[] luceneIndices;


	private final LuceneMultiSearcher multiSearcher = new AdaptiveLuceneMultiSearcher();

	public LLLocalMultiLuceneIndex(Path lucene,
			String name,
			int instancesCount,
			IndicizerAnalyzers indicizerAnalyzers,
			IndicizerSimilarities indicizerSimilarities,
			LuceneOptions luceneOptions) throws IOException {

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
					indicizerAnalyzers,
					indicizerSimilarities,
					luceneOptions
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

	private Flux<Send<LLIndexContext>> getIndexContexts(LLSnapshot snapshot,
			Function<LLLocalLuceneIndex, LLSearchTransformer> indexQueryTransformers) {
		return Flux
				.fromArray(luceneIndices)
				.index()
				// Resolve the snapshot of each shard
				.flatMap(tuple -> Mono
						.fromCallable(() -> resolveSnapshotOptional(snapshot, (int) (long) tuple.getT1()))
						.flatMap(luceneSnapshot -> tuple.getT2().retrieveContext(
								luceneSnapshot.orElse(null), indexQueryTransformers.apply(tuple.getT2()))
						)
				);
	}

	@Override
	public Mono<Void> addDocument(LLTerm id, LLDocument doc) {
		return getLuceneIndex(id).addDocument(id, doc);
	}

	@SuppressWarnings({"unchecked"})
	@Override
	public Mono<Void> addDocuments(Flux<Entry<LLTerm, LLDocument>> documents) {
		return documents
				.transform(normal -> new BufferTimeOutPublisher<>(normal, 512, Duration.ofSeconds(2)))
				.flatMap(inputEntries -> {
					List<Entry<LLTerm, LLDocument>>[] sortedEntries = new List[luceneIndices.length];
					Mono<Void>[] results = new Mono[luceneIndices.length];

					// Sort entries
					for(var inputEntry : inputEntries) {
						int luceneIndexId = getLuceneIndexId(inputEntry.getKey());
						if (sortedEntries[luceneIndexId] == null) {
							sortedEntries[luceneIndexId] = new ArrayList<>();
						}
						sortedEntries[luceneIndexId].add(inputEntry);
					}

					// Add documents
					int luceneIndexId = 0;
					for (List<Entry<LLTerm, LLDocument>> docs : sortedEntries) {
						if (docs != null && !docs.isEmpty()) {
							LLLocalLuceneIndex luceneIndex = luceneIndices[luceneIndexId];
							results[luceneIndexId] = luceneIndex.addDocuments(Flux.fromIterable(docs));
						} else {
							results[luceneIndexId] = Mono.empty();
						}
						luceneIndexId++;
					}

					return Mono.when(results);
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
	public Mono<Void> updateDocuments(Mono<Map<LLTerm, LLDocument>> documents) {
		return documents
				.flatMapMany(map -> {
					var sortedMap = new HashMap<LLLocalLuceneIndex, Map<LLTerm, LLDocument>>();
					map.forEach((key, value) -> sortedMap
							.computeIfAbsent(getLuceneIndex(key), _unused -> new HashMap<>())
							.put(key, value)
					);
					return Flux.fromIterable(Collections.unmodifiableMap(sortedMap).entrySet());
				})
				.flatMap(luceneIndexWithNewDocuments -> {
					var luceneIndex = luceneIndexWithNewDocuments.getKey();
					var docs = luceneIndexWithNewDocuments.getValue();
					return luceneIndex.updateDocuments(Mono.just(docs));
				})
				.then();
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
	public Mono<Send<LLSearchResultShard>> moreLikeThis(@Nullable LLSnapshot snapshot,
			QueryParams queryParams,
			String keyFieldName,
			Flux<Tuple2<String, Set<String>>> mltDocumentFields) {
		LocalQueryParams localQueryParams = LuceneUtils.toLocalQueryParams(queryParams);
		Flux<Send<LLIndexContext>> serchers = this
				.getIndexContexts(snapshot, luceneIndex -> LLSearchTransformer.NO_TRANSFORMATION);

		// Collect all the shards results into a single global result
		return multiSearcher
				.collect(serchers, localQueryParams, keyFieldName)
				// Transform the result type
				.map(resultToReceive -> {
					var result = resultToReceive.receive();
					return new LLSearchResultShard(result.results(), result.totalHitsCount(),
							d -> result.close()).send();
				});

		return multiSearcher
				// Create shard searcher
				.createShardSearcher(localQueryParams)
				.flatMap(shardSearcher -> Flux
						// Iterate the indexed shards
						.fromArray(luceneIndices).index()
						// Resolve the snapshot of each shard
						.flatMap(tuple -> Mono
								.fromCallable(() -> resolveSnapshotOptional(snapshot, (int) (long) tuple.getT1()))
								.map(luceneSnapshot -> new LuceneIndexWithSnapshot(tuple.getT2(), luceneSnapshot))
						)
						// Execute the query and collect it using the shard searcher
						.flatMap(luceneIndexWithSnapshot -> luceneIndexWithSnapshot.luceneIndex()
								.distributedMoreLikeThis(luceneIndexWithSnapshot.snapshot.orElse(null), queryParams, mltDocumentFields, shardSearcher))
						// Collect all the shards results into a single global result
						.then(shardSearcher.collect(localQueryParams, keyFieldName, luceneSearcherScheduler))
				)
				// Fix the result type
				.map(result -> new LLSearchResultShard(result.results(), result.totalHitsCount(), result.release()));
	}

	@Override
	public Mono<Send<LLSearchResultShard>> search(@Nullable LLSnapshot snapshot,
			QueryParams queryParams,
			String keyFieldName) {
		LocalQueryParams localQueryParams = LuceneUtils.toLocalQueryParams(queryParams);
		Flux<Send<LLIndexSearcher>> serchers = getIndexContexts(snapshot);

		// Collect all the shards results into a single global result
		return multiSearcher
				.collect(serchers, localQueryParams, keyFieldName)
				// Transform the result type
				.map(resultToReceive -> {
					var result = resultToReceive.receive();
					return new LLSearchResultShard(result.results(), result.totalHitsCount(),
							d -> result.close()).send();
				});
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
	public Mono<Void> refresh(boolean force) {
		return Flux
				.fromArray(luceneIndices)
				.flatMap(index -> index.refresh(force))
				.then();
	}

	@Override
	public Mono<LLSnapshot> takeSnapshot() {
		return Mono
				// Generate next snapshot index
				.fromCallable(nextSnapshotNumber::getAndIncrement)
				.flatMap(snapshotIndex -> Flux
						.fromArray(luceneIndices)
						.flatMapSequential(LLLocalLuceneIndex::takeSnapshot)
						.collectList()
						.map(list -> list.toArray(LLSnapshot[]::new))
						.doOnNext(instancesSnapshotsArray -> registeredSnapshots.put(snapshotIndex, instancesSnapshotsArray))
						.thenReturn(new LLSnapshot(snapshotIndex))
				);
	}

	@Override
	public Mono<Void> releaseSnapshot(LLSnapshot snapshot) {
		return Mono
				.fromCallable(() -> registeredSnapshots.remove(snapshot.getSequenceNumber()))
				.flatMapMany(Flux::fromArray)
				.index()
				.flatMapSequential(tuple -> {
					int index = (int) (long) tuple.getT1();
					LLSnapshot instanceSnapshot = tuple.getT2();
					return luceneIndices[index].releaseSnapshot(instanceSnapshot);
				})
				.then();
	}

	@Override
	public boolean isLowMemoryMode() {
		return luceneIndices[0].isLowMemoryMode();
	}

	private class MoreLikeThisTransformer implements LLSearchTransformer {

		private final LLLocalLuceneIndex luceneIndex;
		private final LLSnapshot snapshot;
		private final String keyFieldName;
		private final Flux<Tuple2<String, Set<String>>> mltDocumentFields;

		public MoreLikeThisTransformer(LLLocalLuceneIndex luceneIndex,
				@Nullable LLSnapshot snapshot,
				String keyFieldName,
				Flux<Tuple2<String, Set<String>>> mltDocumentFields) {
			this.luceneIndex = luceneIndex;
			this.snapshot = snapshot;
			this.keyFieldName = keyFieldName;
			this.mltDocumentFields = mltDocumentFields;
		}

		@Override
		public Mono<LocalQueryParams> transform(Mono<LocalQueryParams> queryParamsMono) {
			return queryParamsMono
					.flatMap(queryParams -> {
						luceneIndex.getMoreLikeThisTransformer(snapshot, queryParams, mltDocumentFields, );
					});
			LLLocalMultiLuceneIndex.this.
			return null;
		}
	}
}
