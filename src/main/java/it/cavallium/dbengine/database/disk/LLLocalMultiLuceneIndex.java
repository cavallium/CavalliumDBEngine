package it.cavallium.dbengine.database.disk;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader.InvalidCacheLoadException;
import it.cavallium.dbengine.client.IndicizerAnalyzers;
import it.cavallium.dbengine.client.IndicizerSimilarities;
import it.cavallium.dbengine.client.query.current.data.QueryParams;
import it.cavallium.dbengine.database.LLDocument;
import it.cavallium.dbengine.database.LLLuceneIndex;
import it.cavallium.dbengine.database.LLSearchResult;
import it.cavallium.dbengine.database.LLSearchResultShard;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.LLTerm;
import it.cavallium.dbengine.lucene.analyzer.TextFieldsAnalyzer;
import it.cavallium.dbengine.lucene.analyzer.TextFieldsSimilarity;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectArrayMap;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.IndexSearcher;
import org.jetbrains.annotations.Nullable;
import org.warp.commonutils.batch.ParallelUtils;
import org.warp.commonutils.functional.IOBiConsumer;
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

	private final AtomicLong nextActionId = new AtomicLong(0);
	private final ConcurrentHashMap<Long, Cache<String, Optional<CollectionStatistics>>[]> statistics = new ConcurrentHashMap<>();
	private final ConcurrentHashMap<Long, AtomicInteger> completedStreams = new ConcurrentHashMap<>();

	private final int maxQueueSize = 1000;

	public LLLocalMultiLuceneIndex(Path lucene,
			String name,
			int instancesCount,
			IndicizerAnalyzers indicizerAnalyzers,
			IndicizerSimilarities indicizerSimilarities,
			Duration queryRefreshDebounceTime,
			Duration commitDebounceTime,
			boolean lowMemory, boolean inMemory) throws IOException {

		if (instancesCount <= 1 || instancesCount > 100) {
			throw new IOException("Unsupported instances count: " + instancesCount);
		}

		LLLocalLuceneIndex[] luceneIndices = new LLLocalLuceneIndex[instancesCount];
		for (int i = 0; i < instancesCount; i++) {
			int finalI = i;
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
					queryRefreshDebounceTime,
					commitDebounceTime,
					lowMemory, inMemory, (indexSearcher, field, distributedPre, actionId) -> distributedCustomCollectionStatistics(finalI,
							indexSearcher,
							field,
							distributedPre,
							actionId
					)
			);
		}
		this.luceneIndices = luceneIndices;
	}

	private long newAction() {
		var statistics = new Cache[luceneIndices.length];
		for (int i = 0; i < luceneIndices.length; i++) {
			statistics[i] = CacheBuilder.newBuilder().build();
		}
		long actionId = nextActionId.getAndIncrement();
		//noinspection unchecked
		this.statistics.put(actionId, statistics);
		this.completedStreams.put(actionId, new AtomicInteger(0));
		return actionId;
	}

	private void completedAction(long actionId) {
		var completedStreamsCount = completedStreams.get(actionId);
		if (completedStreamsCount != null) {
			if (completedStreamsCount.incrementAndGet() >= luceneIndices.length) {
				this.statistics.remove(actionId);
				this.completedStreams.remove(actionId);
			}
		}
	}

	private CollectionStatistics distributedCustomCollectionStatistics(int luceneIndex,
			IndexSearcher indexSearcher, String field, boolean distributedPre, long actionId) throws IOException {
		if (distributedPre) {
			try {
				var optional = statistics.get(actionId)[luceneIndex].get(field,
						() -> Optional.ofNullable(indexSearcher.collectionStatistics(field))
				);
				return optional.orElse(null);
			} catch ( InvalidCacheLoadException | ExecutionException e) {
				throw new IOException(e);
			}
		} else {
			long maxDoc = 0;
			long docCount = 0;
			long sumTotalTermFreq = 0;
			long sumDocFreq = 0;
			for (int i = 0; i < luceneIndices.length; i++) {
				CollectionStatistics iCollStats = Objects
						.requireNonNull(statistics.get(actionId)[i].getIfPresent(field))
						.orElse(null);
				if (iCollStats != null) {
					maxDoc += iCollStats.maxDoc();
					docCount += iCollStats.docCount();
					sumTotalTermFreq += iCollStats.sumTotalTermFreq();
					sumDocFreq += iCollStats.sumDocFreq();
				}
			}

			return new CollectionStatistics(field,
					(int) Math.max(1, Math.min(maxDoc, Integer.MAX_VALUE)),
					Math.max(1, docCount),
					Math.max(1, sumTotalTermFreq),
					Math.max(1, sumDocFreq)
			);
		}
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

	@SuppressWarnings({"unchecked"})
	@Override
	public Mono<Void> addDocuments(Flux<Entry<LLTerm, LLDocument>> documents) {
		return documents
				.bufferTimeout(512, Duration.ofSeconds(2))
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
					return Flux.fromIterable(sortedMap.entrySet());
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
	public Mono<LLSearchResult> moreLikeThis(@Nullable LLSnapshot snapshot,
			QueryParams queryParams,
			String keyFieldName,
			Flux<Tuple2<String, Set<String>>> mltDocumentFields) {
		if (queryParams.getOffset() != 0) {
			return Mono.error(new IllegalArgumentException("MultiLuceneIndex requires an offset equal to 0"));
		}
		Flux<Tuple2<String, Set<String>>> mltDocumentFieldsShared;
		Mono<DistributedSearch> distributedPre;
		if (luceneIndices.length > 1) {
			long actionId = newAction();
			mltDocumentFieldsShared = mltDocumentFields.publish().refCount();
			distributedPre = Flux
					.fromArray(luceneIndices)
					.index()
					.flatMap(tuple -> Mono
							.fromCallable(() -> resolveSnapshotOptional(snapshot, (int) (long) tuple.getT1()))
							.map(luceneSnapshot -> Tuples.of(tuple.getT2(), luceneSnapshot))
					)
					.flatMap(tuple -> tuple
							.getT1()
							.distributedPreMoreLikeThis(tuple.getT2().orElse(null),
									queryParams,
									keyFieldName,
									mltDocumentFieldsShared,
									actionId
							)
					)
					.then(Mono.just(new DistributedSearch(actionId, 20)));
		} else {
			mltDocumentFieldsShared = mltDocumentFields;
			distributedPre = Mono.just(new DistributedSearch(-1, 1));
		}

		//noinspection DuplicatedCode
		return distributedPre
				.flatMap(distributedSearch -> Flux
						.fromArray(luceneIndices)
						.index()
						.flatMap(tuple -> Mono
								.fromCallable(() -> resolveSnapshotOptional(snapshot, (int) (long) tuple.getT1()))
								.map(luceneSnapshot -> Tuples.of(tuple.getT2(), luceneSnapshot)))
						.flatMap(tuple -> tuple
								.getT1()
								.distributedMoreLikeThis(tuple.getT2().orElse(null),
										queryParams,
										keyFieldName,
										mltDocumentFieldsShared,
										distributedSearch.actionId(),
										distributedSearch.scoreDivisor()
								)
						)
						.reduce(LLSearchResult.accumulator())
						.map(result -> {
							if (distributedSearch.actionId() != -1) {
								Flux<LLSearchResultShard> resultsWithTermination = result
										.results()
										.map(flux -> new LLSearchResultShard(Flux
												.using(
														distributedSearch::actionId,
														actionId -> flux.results(),
														this::completedAction
												), flux.totalHitsCount())
										);
								return new LLSearchResult(resultsWithTermination);
							} else {
								return result;
							}
						})
						.doOnError(ex -> {
							if (distributedSearch.actionId() != -1) {
								completedAction(distributedSearch.actionId());
							}
						})
				);
	}

	private static record DistributedSearch(long actionId, int scoreDivisor) {}

	@Override
	public Mono<LLSearchResult> search(@Nullable LLSnapshot snapshot,
			QueryParams queryParams,
			String keyFieldName) {
		if (queryParams.getOffset() != 0) {
			return Mono.error(new IllegalArgumentException("MultiLuceneIndex requires an offset equal to 0"));
		}

		Mono<DistributedSearch> distributedSearchMono;
		if (luceneIndices.length <= 1 || !queryParams.getScoreMode().getComputeScores()) {
			distributedSearchMono = Mono.just(new DistributedSearch(-1, 1));
		} else {
			var actionId = newAction();
			distributedSearchMono = Flux
					.fromArray(luceneIndices)
					.index()
					.flatMap(tuple -> Mono
							.fromCallable(() -> resolveSnapshotOptional(snapshot, (int) (long) tuple.getT1()))
							.map(luceneSnapshot -> Tuples.of(tuple.getT2(), luceneSnapshot))
					)
					.flatMap(tuple -> tuple
							.getT1()
							.distributedPreSearch(tuple.getT2().orElse(null),
									queryParams,
									keyFieldName,
									actionId
							)
					)
					.then(Mono.just(new DistributedSearch(actionId, 20)));
		}

		return distributedSearchMono
				.flatMap(distributedSearch -> Flux
						.fromArray(luceneIndices)
						.index()
						.flatMap(tuple -> Mono
								.fromCallable(() -> resolveSnapshotOptional(snapshot, (int) (long) tuple.getT1()))
								.map(luceneSnapshot -> Tuples.of(tuple.getT2(), luceneSnapshot))
						)
						.flatMap(tuple -> tuple
								.getT1()
								.distributedSearch(tuple.getT2().orElse(null),
										queryParams,
										keyFieldName,
										distributedSearch.actionId(),
										distributedSearch.scoreDivisor()
								))
						.reduce(LLSearchResult.accumulator())
						.map(result -> {
							if (distributedSearch.actionId() != -1) {
								Flux<LLSearchResultShard> resultsWithTermination = result
										.results()
										.map(flux -> new LLSearchResultShard(Flux
												.using(
														distributedSearch::actionId,
														actionId -> flux.results(),
														this::completedAction
												), flux.totalHitsCount())
										);
								return new LLSearchResult(resultsWithTermination);
							} else {
								return result;
							}
						})
						.doOnError(ex -> {
							if (distributedSearch.actionId() != -1) {
								completedAction(distributedSearch.actionId());
							}
						})
				);
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

	@Override
	public boolean supportsOffset() {
		return false;
	}
}
