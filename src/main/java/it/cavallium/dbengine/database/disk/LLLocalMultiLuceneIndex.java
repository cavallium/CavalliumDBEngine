package it.cavallium.dbengine.database.disk;

import static it.cavallium.dbengine.client.UninterruptibleScheduler.uninterruptibleScheduler;

import com.google.common.collect.Multimap;
import io.micrometer.core.instrument.MeterRegistry;
import io.netty5.buffer.api.Send;
import it.cavallium.dbengine.client.query.QueryParser;
import it.cavallium.dbengine.client.query.current.data.Query;
import it.cavallium.dbengine.client.query.current.data.QueryParams;
import it.cavallium.dbengine.database.LLIndexRequest;
import it.cavallium.dbengine.database.LLLuceneIndex;
import it.cavallium.dbengine.database.LLSearchResultShard;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.LLTerm;
import it.cavallium.dbengine.database.LLUpdateDocument;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.lucene.LuceneHacks;
import it.cavallium.dbengine.lucene.LuceneRocksDBManager;
import it.cavallium.dbengine.lucene.LuceneUtils;
import it.cavallium.dbengine.lucene.collector.Buckets;
import it.cavallium.dbengine.lucene.mlt.MoreLikeThisTransformer;
import it.cavallium.dbengine.lucene.searcher.AdaptiveMultiSearcher;
import it.cavallium.dbengine.lucene.searcher.BucketParams;
import it.cavallium.dbengine.lucene.searcher.DecimalBucketMultiSearcher;
import it.cavallium.dbengine.lucene.searcher.GlobalQueryRewrite;
import it.cavallium.dbengine.lucene.searcher.LocalQueryParams;
import it.cavallium.dbengine.lucene.searcher.MultiSearcher;
import it.cavallium.dbengine.rpc.current.data.IndicizerAnalyzers;
import it.cavallium.dbengine.rpc.current.data.IndicizerSimilarities;
import it.cavallium.dbengine.rpc.current.data.LuceneOptions;
import it.unimi.dsi.fastutil.ints.IntList;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.search.similarities.PerFieldSimilarityWrapper;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class LLLocalMultiLuceneIndex implements LLLuceneIndex {

	private static final Logger LOG = LogManager.getLogger(LLLuceneIndex.class);
	private static final boolean BYPASS_GROUPBY_BUG = Boolean.parseBoolean(System.getProperty(
			"it.cavallium.dbengine.bypassGroupByBug",
			"false"
	));

	static {
		LLUtils.initHooks();
	}

	private final String clusterName;
	private final boolean lowMemory;
	private final MeterRegistry meterRegistry;
	private final ConcurrentHashMap<Long, List<LLSnapshot>> registeredSnapshots = new ConcurrentHashMap<>();
	private final AtomicLong nextSnapshotNumber = new AtomicLong(1);
	private final LLLocalLuceneIndex[] luceneIndicesById;
	private final List<LLLocalLuceneIndex> luceneIndicesSet;
	private final int totalShards;
	private final Flux<LLLocalLuceneIndex> luceneIndicesFlux;
	private final PerFieldAnalyzerWrapper luceneAnalyzer;
	private final PerFieldSimilarityWrapper luceneSimilarity;

	private final MultiSearcher multiSearcher;
	private final DecimalBucketMultiSearcher decimalBucketMultiSearcher = new DecimalBucketMultiSearcher();

	public LLLocalMultiLuceneIndex(LLTempLMDBEnv env,
			MeterRegistry meterRegistry,
			String clusterName,
			IntList activeShards,
			int totalShards,
			IndicizerAnalyzers indicizerAnalyzers,
			IndicizerSimilarities indicizerSimilarities,
			LuceneOptions luceneOptions,
			@Nullable LuceneHacks luceneHacks,
			LuceneRocksDBManager rocksDBManager) throws IOException {

		if (totalShards <= 1 || totalShards > 100) {
			throw new IOException("Unsupported instances count: " + totalShards);
		}

		this.meterRegistry = meterRegistry;
		LLLocalLuceneIndex[] luceneIndices = new LLLocalLuceneIndex[totalShards];
		for (int i = 0; i < totalShards; i++) {
			if (!activeShards.contains(i)) {
				continue;
			}
			luceneIndices[i] = new LLLocalLuceneIndex(env,
					meterRegistry,
					clusterName,
					i,
					indicizerAnalyzers,
					indicizerSimilarities,
					luceneOptions,
					luceneHacks,
					rocksDBManager
			);
		}
		this.clusterName = clusterName;
		this.totalShards = totalShards;
		this.luceneIndicesById = luceneIndices;
		var luceneIndicesSet = new HashSet<LLLocalLuceneIndex>();
		for (var luceneIndex : luceneIndices) {
			if (luceneIndex != null) {
				luceneIndicesSet.add(luceneIndex);
			}
		}
		this.luceneIndicesSet = new ArrayList<>(luceneIndicesSet);
		this.luceneIndicesFlux = Flux.fromIterable(luceneIndicesSet);
		this.luceneAnalyzer = LuceneUtils.toPerFieldAnalyzerWrapper(indicizerAnalyzers);
		this.luceneSimilarity = LuceneUtils.toPerFieldSimilarityWrapper(indicizerSimilarities);
		this.lowMemory = luceneOptions.lowMemory();

		var useLMDB = luceneOptions.allowNonVolatileCollection();
		var maxInMemoryResultEntries = luceneOptions.maxInMemoryResultEntries();
		if (luceneHacks != null && luceneHacks.customMultiSearcher() != null) {
			multiSearcher = luceneHacks.customMultiSearcher().get();
		} else {
			multiSearcher = new AdaptiveMultiSearcher(env, useLMDB, maxInMemoryResultEntries);
		}
	}

	private LLLocalLuceneIndex getLuceneIndex(LLTerm id) {
		return Objects.requireNonNull(luceneIndicesById[LuceneUtils.getLuceneIndexId(id, totalShards)]);
	}

	@Override
	public String getLuceneIndexName() {
		return clusterName;
	}

	private Mono<Send<LLIndexSearchers>> getIndexSearchers(LLSnapshot snapshot) {
		return luceneIndicesFlux
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

	@Override
	public Mono<Void> addDocuments(boolean atomic, Flux<Entry<LLTerm, LLUpdateDocument>> documents) {
		if (BYPASS_GROUPBY_BUG) {
			return documents
					.buffer(8192)
					.flatMap(inputEntries -> {
						List<Entry<LLTerm, LLUpdateDocument>>[] sortedEntries = new List[totalShards];
						Mono<Void>[] results = new Mono[totalShards];

						// Sort entries
						for(var inputEntry : inputEntries) {
							int luceneIndexId = LuceneUtils.getLuceneIndexId(inputEntry.getKey(), totalShards);
							if (sortedEntries[luceneIndexId] == null) {
								sortedEntries[luceneIndexId] = new ArrayList<>();
							}
							sortedEntries[luceneIndexId].add(inputEntry);
						}

						// Add documents
						int luceneIndexId = 0;
						for (List<Entry<LLTerm, LLUpdateDocument>> docs : sortedEntries) {
							if (docs != null && !docs.isEmpty()) {
								LLLocalLuceneIndex luceneIndex = Objects.requireNonNull(luceneIndicesById[luceneIndexId]);
								results[luceneIndexId] = luceneIndex.addDocuments(atomic, Flux.fromIterable(docs));
							} else {
								results[luceneIndexId] = Mono.empty();
							}
							luceneIndexId++;
						}

						return Mono.when(results);
					})
					.then();
		} else {
			return documents
					.groupBy(term -> getLuceneIndex(term.getKey()))
					.flatMap(group -> group.key().addDocuments(atomic, group))
					.then();
		}
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
	public Mono<Void> updateDocuments(Flux<Entry<LLTerm, LLUpdateDocument>> documents) {
		if (BYPASS_GROUPBY_BUG) {
			return documents
					.buffer(8192)
					.flatMap(inputEntries -> {
						List<Entry<LLTerm, LLUpdateDocument>>[] sortedEntries = new List[totalShards];
						Mono<Void>[] results = new Mono[totalShards];

						// Sort entries
						for(var inputEntry : inputEntries) {
							int luceneIndexId = LuceneUtils.getLuceneIndexId(inputEntry.getKey(), totalShards);
							if (sortedEntries[luceneIndexId] == null) {
								sortedEntries[luceneIndexId] = new ArrayList<>();
							}
							sortedEntries[luceneIndexId].add(inputEntry);
						}

						// Add documents
						int luceneIndexId = 0;
						for (List<Entry<LLTerm, LLUpdateDocument>> docs : sortedEntries) {
							if (docs != null && !docs.isEmpty()) {
								LLLocalLuceneIndex luceneIndex = Objects.requireNonNull(luceneIndicesById[luceneIndexId]);
								results[luceneIndexId] = luceneIndex.updateDocuments(Flux.fromIterable(docs));
							} else {
								results[luceneIndexId] = Mono.empty();
							}
							luceneIndexId++;
						}

						return Mono.when(results);
					})
					.then();
		} else {
			return documents
					.groupBy(term -> getLuceneIndex(term.getKey()))
					.flatMap(group -> group.key().updateDocuments(group))
					.then();
		}
	}

	@Override
	public Mono<Void> deleteAll() {
		return luceneIndicesFlux
				.flatMap(LLLocalLuceneIndex::deleteAll)
				.then();
	}

	private LLSnapshot resolveSnapshot(LLSnapshot multiSnapshot, int instanceId) {
		if (multiSnapshot != null) {
			return registeredSnapshots.get(multiSnapshot.getSequenceNumber()).get(instanceId);
		} else {
			return null;
		}
	}

	private Optional<LLSnapshot> resolveSnapshotOptional(LLSnapshot multiSnapshot, int instanceId) {
		return Optional.ofNullable(resolveSnapshot(multiSnapshot, instanceId));
	}

	@Override
	public Flux<LLSearchResultShard> moreLikeThis(@Nullable LLSnapshot snapshot,
			QueryParams queryParams,
			String keyFieldName,
			Multimap<String, String> mltDocumentFields) {
		LocalQueryParams localQueryParams = LuceneUtils.toLocalQueryParams(queryParams, luceneAnalyzer);
		var searchers = this.getIndexSearchers(snapshot);
		var transformer = new MoreLikeThisTransformer(mltDocumentFields, luceneAnalyzer, luceneSimilarity);

		// Collect all the shards results into a single global result
		return multiSearcher
				.collectMulti(searchers, localQueryParams, keyFieldName, transformer)
				// Transform the result type
				.map(result -> new LLSearchResultShard(result.results(), result.totalHitsCount(), result::close))
				.flux();
	}

	@Override
	public Flux<LLSearchResultShard> search(@Nullable LLSnapshot snapshot,
			QueryParams queryParams,
			@Nullable String keyFieldName) {
		LocalQueryParams localQueryParams = LuceneUtils.toLocalQueryParams(queryParams, luceneAnalyzer);
		var searchers = getIndexSearchers(snapshot);

		// Collect all the shards results into a single global result
		return multiSearcher
				.collectMulti(searchers, localQueryParams, keyFieldName, GlobalQueryRewrite.NO_REWRITE)
				// Transform the result type
				.map(result -> new LLSearchResultShard(result.results(), result.totalHitsCount(), result::close))
				.flux();
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
		return decimalBucketMultiSearcher.collectMulti(searchers, bucketParams, localQueries, localNormalizationQuery);
	}

	@Override
	public Mono<Void> close() {
		return luceneIndicesFlux
				.flatMap(LLLocalLuceneIndex::close)
				.then(Mono.fromCallable(() -> {
					if (multiSearcher instanceof Closeable closeable) {
						//noinspection BlockingMethodInNonBlockingContext
						closeable.close();
					}
					return null;
				}).subscribeOn(uninterruptibleScheduler(Schedulers.boundedElastic())))
				.publishOn(Schedulers.parallel())
				.then();
	}

	@Override
	public Mono<Void> flush() {
		return luceneIndicesFlux
				.flatMap(LLLocalLuceneIndex::flush)
				.then();
	}

	@Override
	public Mono<Void> refresh(boolean force) {
		return luceneIndicesFlux
				.flatMap(index -> index.refresh(force))
				.then();
	}

	@Override
	public Mono<LLSnapshot> takeSnapshot() {
		return Mono
				// Generate next snapshot index
				.fromCallable(nextSnapshotNumber::getAndIncrement)
				.flatMap(snapshotIndex -> luceneIndicesFlux
						.flatMapSequential(LLLocalLuceneIndex::takeSnapshot)
						.collectList()
						.doOnNext(instancesSnapshotsArray -> registeredSnapshots.put(snapshotIndex, instancesSnapshotsArray))
						.thenReturn(new LLSnapshot(snapshotIndex))
				);
	}

	@Override
	public Mono<Void> releaseSnapshot(LLSnapshot snapshot) {
		return Mono
				.fromCallable(() -> registeredSnapshots.remove(snapshot.getSequenceNumber()))
				.flatMapIterable(list -> list)
				.index()
				.flatMapSequential(tuple -> {
					int index = (int) (long) tuple.getT1();
					LLSnapshot instanceSnapshot = tuple.getT2();
					return luceneIndicesSet.get(index).releaseSnapshot(instanceSnapshot);
				})
				.then();
	}

	@Override
	public boolean isLowMemoryMode() {
		return lowMemory;
	}
}
