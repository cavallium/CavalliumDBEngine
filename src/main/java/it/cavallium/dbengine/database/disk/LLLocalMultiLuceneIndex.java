package it.cavallium.dbengine.database.disk;

import io.micrometer.core.instrument.MeterRegistry;
import io.net5.buffer.api.Resource;
import io.net5.buffer.api.Send;
import it.cavallium.dbengine.client.IndicizerAnalyzers;
import it.cavallium.dbengine.client.IndicizerSimilarities;
import it.cavallium.dbengine.client.LuceneOptions;
import it.cavallium.dbengine.client.query.QueryParser;
import it.cavallium.dbengine.client.query.current.data.Query;
import it.cavallium.dbengine.client.query.current.data.QueryParams;
import it.cavallium.dbengine.database.LLIndexRequest;
import it.cavallium.dbengine.database.LLUpdateDocument;
import it.cavallium.dbengine.database.LLItem;
import it.cavallium.dbengine.database.LLLuceneIndex;
import it.cavallium.dbengine.database.LLSearchResultShard;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.LLTerm;
import it.cavallium.dbengine.lucene.LuceneHacks;
import it.cavallium.dbengine.lucene.LuceneUtils;
import it.cavallium.dbengine.lucene.collector.Buckets;
import it.cavallium.dbengine.lucene.searcher.AdaptiveMultiSearcher;
import it.cavallium.dbengine.lucene.searcher.BucketParams;
import it.cavallium.dbengine.lucene.searcher.DecimalBucketMultiSearcher;
import it.cavallium.dbengine.lucene.searcher.LLSearchTransformer;
import it.cavallium.dbengine.lucene.searcher.LocalQueryParams;
import it.cavallium.dbengine.lucene.searcher.MultiSearcher;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
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
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.search.similarities.PerFieldSimilarityWrapper;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.function.Tuple2;

public class LLLocalMultiLuceneIndex implements LLLuceneIndex {

	private final MeterRegistry meterRegistry;
	private final ConcurrentHashMap<Long, LLSnapshot[]> registeredSnapshots = new ConcurrentHashMap<>();
	private final AtomicLong nextSnapshotNumber = new AtomicLong(1);
	private final LLLocalLuceneIndex[] luceneIndices;
	private final PerFieldAnalyzerWrapper luceneAnalyzer;
	private final PerFieldSimilarityWrapper luceneSimilarity;

	private final MultiSearcher multiSearcher;
	private final DecimalBucketMultiSearcher decimalBucketMultiSearcher = new DecimalBucketMultiSearcher();

	public LLLocalMultiLuceneIndex(LLTempLMDBEnv env,
			Path lucene,
			MeterRegistry meterRegistry,
			String name,
			int instancesCount,
			IndicizerAnalyzers indicizerAnalyzers,
			IndicizerSimilarities indicizerSimilarities,
			LuceneOptions luceneOptions,
			@Nullable LuceneHacks luceneHacks) throws IOException {

		if (instancesCount <= 1 || instancesCount > 100) {
			throw new IOException("Unsupported instances count: " + instancesCount);
		}

		this.meterRegistry = meterRegistry;
		LLLocalLuceneIndex[] luceneIndices = new LLLocalLuceneIndex[instancesCount];
		for (int i = 0; i < instancesCount; i++) {
			String instanceName;
			if (i == 0) {
				instanceName = name;
			} else {
				instanceName = name + "_" + String.format("%03d", i);
			}
			luceneIndices[i] = new LLLocalLuceneIndex(env,
					lucene,
					meterRegistry,
					instanceName,
					indicizerAnalyzers,
					indicizerSimilarities,
					luceneOptions,
					luceneHacks
			);
		}
		this.luceneIndices = luceneIndices;
		this.luceneAnalyzer = LuceneUtils.toPerFieldAnalyzerWrapper(indicizerAnalyzers);
		this.luceneSimilarity = LuceneUtils.toPerFieldSimilarityWrapper(indicizerSimilarities);

		var useLMDB = luceneOptions.allowNonVolatileCollection();
		if (luceneHacks != null && luceneHacks.customMultiSearcher() != null) {
			multiSearcher = luceneHacks.customMultiSearcher().get();
		} else {
			multiSearcher = new AdaptiveMultiSearcher(env, useLMDB);
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

	private Mono<Send<LLIndexSearchers>> getIndexSearchers(LLSnapshot snapshot) {
		return Flux
				.fromArray(luceneIndices)
				.index()
				// Resolve the snapshot of each shard
				.flatMap(tuple -> Mono
						.fromCallable(() -> resolveSnapshotOptional(snapshot, (int) (long) tuple.getT1()))
						.flatMap(luceneSnapshot -> tuple.getT2().retrieveSearcher(luceneSnapshot.orElse(null)))
				)
				.collectList()
				.map(searchers -> LLIndexSearchers.of(searchers).send());
	}

	@Override
	public Mono<Void> addDocument(LLTerm id, LLUpdateDocument doc) {
		return getLuceneIndex(id).addDocument(id, doc);
	}

	@SuppressWarnings({"unchecked"})
	@Override
	public Mono<Void> addDocuments(Flux<Entry<LLTerm, LLUpdateDocument>> documents) {
		return documents
				.buffer(512)
				.flatMap(inputEntries -> {
					List<Entry<LLTerm, LLUpdateDocument>>[] sortedEntries = new List[luceneIndices.length];
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
					for (List<Entry<LLTerm, LLUpdateDocument>> docs : sortedEntries) {
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
	public Mono<Void> update(LLTerm id, LLIndexRequest request) {
		return getLuceneIndex(id).update(id, request);
	}

	@Override
	public Mono<Void> updateDocuments(Mono<Map<LLTerm, LLUpdateDocument>> documents) {
		return documents
				.flatMapMany(map -> {
					var sortedMap = new HashMap<LLLocalLuceneIndex, Map<LLTerm, LLUpdateDocument>>();
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
	public Mono<LLSearchResultShard> moreLikeThis(@Nullable LLSnapshot snapshot,
			QueryParams queryParams,
			String keyFieldName,
			Flux<Tuple2<String, Set<String>>> mltDocumentFields) {
		LocalQueryParams localQueryParams = LuceneUtils.toLocalQueryParams(queryParams, luceneAnalyzer);
		var searchers = this.getIndexSearchers(snapshot);
		var transformer = new MultiMoreLikeThisTransformer(mltDocumentFields);

		// Collect all the shards results into a single global result
		return multiSearcher
				.collectMulti(searchers, localQueryParams, keyFieldName, transformer)
				// Transform the result type
				.map(result -> new LLSearchResultShard(result.results(), result.totalHitsCount(), result::close))
				.doOnDiscard(Send.class, Send::close)
				.doOnDiscard(Resource.class, Resource::close);
	}

	@Override
	public Mono<LLSearchResultShard> search(@Nullable LLSnapshot snapshot,
			QueryParams queryParams,
			String keyFieldName) {
		LocalQueryParams localQueryParams = LuceneUtils.toLocalQueryParams(queryParams, luceneAnalyzer);
		var searchers = getIndexSearchers(snapshot);

		// Collect all the shards results into a single global result
		return multiSearcher
				.collectMulti(searchers, localQueryParams, keyFieldName, LLSearchTransformer.NO_TRANSFORMATION)
				// Transform the result type
				.map(result -> new LLSearchResultShard(result.results(), result.totalHitsCount(), result::close))
				.doOnDiscard(Send.class, Send::close).doOnDiscard(Resource.class, Resource::close);
	}

	@Override
	public Mono<Buckets> computeBuckets(@Nullable LLSnapshot snapshot,
			@NotNull List<Query> queries,
			@Nullable Query normalizationQuery,
			BucketParams bucketParams) {
		List<org.apache.lucene.search.Query> localQueries = new ArrayList<>(queries.size());
		for (Query query : queries) {
			localQueries.add(QueryParser.toQuery(query, luceneAnalyzer));
		}
		var localNormalizationQuery = QueryParser.toQuery(normalizationQuery, luceneAnalyzer);
		var searchers = getIndexSearchers(snapshot);

		// Collect all the shards results into a single global result
		return decimalBucketMultiSearcher
				.collectMulti(searchers, bucketParams, localQueries, localNormalizationQuery)
				.doOnDiscard(Send.class, Send::close)
				.doOnDiscard(Resource.class, Resource::close);
	}

	@Override
	public Mono<Void> close() {
		return Flux
				.fromArray(luceneIndices)
				.flatMap(LLLocalLuceneIndex::close)
				.then(Mono.fromCallable(() -> {
					if (multiSearcher instanceof Closeable closeable) {
						closeable.close();
					}
					return null;
				}).subscribeOn(Schedulers.boundedElastic()))
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

	private class MultiMoreLikeThisTransformer implements LLSearchTransformer {

		private final Flux<Tuple2<String, Set<String>>> mltDocumentFields;

		public MultiMoreLikeThisTransformer(Flux<Tuple2<String, Set<String>>> mltDocumentFields) {
			this.mltDocumentFields = mltDocumentFields;
		}

		@Override
		public Mono<LocalQueryParams> transform(Mono<TransformerInput> inputMono) {
			return inputMono.flatMap(input -> LuceneUtils.getMoreLikeThisQuery(input.indexSearchers(), input.queryParams(),
					luceneAnalyzer, luceneSimilarity, mltDocumentFields));
		}
	}
}
