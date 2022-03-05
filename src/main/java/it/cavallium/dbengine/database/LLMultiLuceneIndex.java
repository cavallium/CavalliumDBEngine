package it.cavallium.dbengine.database;

import com.google.common.collect.Multimap;
import io.net5.buffer.api.Resource;
import it.cavallium.dbengine.rpc.current.data.IndicizerAnalyzers;
import it.cavallium.dbengine.rpc.current.data.IndicizerSimilarities;
import it.cavallium.dbengine.client.query.current.data.Query;
import it.cavallium.dbengine.client.query.current.data.QueryParams;
import it.cavallium.dbengine.client.query.current.data.TotalHitsCount;
import it.cavallium.dbengine.lucene.LuceneHacks;
import it.cavallium.dbengine.lucene.LuceneUtils;
import it.cavallium.dbengine.lucene.collector.Buckets;
import it.cavallium.dbengine.lucene.searcher.BucketParams;
import it.cavallium.dbengine.rpc.current.data.LuceneIndexStructure;
import it.cavallium.dbengine.rpc.current.data.LuceneOptions;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class LLMultiLuceneIndex implements LLLuceneIndex {


	private final ConcurrentHashMap<Long, List<LLSnapshot>> registeredSnapshots = new ConcurrentHashMap<>();
	private final AtomicLong nextSnapshotNumber = new AtomicLong(1);

	private final String clusterName;
	private final LuceneIndexStructure indexStructure;
	private final IndicizerAnalyzers indicizerAnalyzers;
	private final IndicizerSimilarities indicizerSimilarities;
	private final LuceneOptions luceneOptions;
	private final LuceneHacks luceneHacks;
	private final LLLuceneIndex[] luceneIndicesById;
	private final List<LLLuceneIndex> luceneIndicesSet;
	private final int totalShards;
	private final Flux<LLLuceneIndex> luceneIndicesFlux;

	public LLMultiLuceneIndex(String clusterName,
			LuceneIndexStructure indexStructure,
			IndicizerAnalyzers indicizerAnalyzers,
			IndicizerSimilarities indicizerSimilarities,
			LuceneOptions luceneOptions,
			LuceneHacks luceneHacks,
			LLLuceneIndex[] luceneIndices) {
		this.clusterName = clusterName;
		this.indexStructure = indexStructure;
		this.indicizerAnalyzers = indicizerAnalyzers;
		this.indicizerSimilarities = indicizerSimilarities;
		this.luceneOptions = luceneOptions;
		this.luceneHacks = luceneHacks;
		this.luceneIndicesById = luceneIndices;
		this.totalShards = indexStructure.totalShards();
		var luceneIndicesSet = new HashSet<LLLuceneIndex>();
		for (LLLuceneIndex luceneIndex : luceneIndices) {
			if (luceneIndex != null) {
				luceneIndicesSet.add(luceneIndex);
			}
		}
		this.luceneIndicesSet = new ArrayList<>(luceneIndicesSet);
		this.luceneIndicesFlux = Flux.fromIterable(luceneIndicesSet);
	}

	@Override
	public String getLuceneIndexName() {
		return clusterName;
	}

	private LLLuceneIndex getLuceneIndex(LLTerm id) {
		return luceneIndicesById[LuceneUtils.getLuceneIndexId(id, totalShards)];
	}

	@Override
	public Mono<Void> addDocument(LLTerm id, LLUpdateDocument doc) {
		return getLuceneIndex(id).addDocument(id, doc);
	}

	@Override
	public Mono<Void> addDocuments(Flux<Entry<LLTerm, LLUpdateDocument>> documents) {
		return documents
				.groupBy(term -> LuceneUtils.getLuceneIndexId(term.getKey(), totalShards))
				.flatMap(group -> {
					var index = luceneIndicesById[group.key()];
					return index.addDocuments(group);
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
	public Mono<Void> updateDocuments(Flux<Entry<LLTerm, LLUpdateDocument>> documents) {
		return documents
				.groupBy(term -> LuceneUtils.getLuceneIndexId(term.getKey(), totalShards))
				.flatMap(group -> {
					var index = luceneIndicesById[group.key()];
					return index.updateDocuments(group);
				})
				.then();
	}

	@Override
	public Mono<Void> deleteAll() {
		return luceneIndicesFlux.flatMap(LLLuceneIndex::deleteAll).then();
	}

	@Override
	public Flux<LLSearchResultShard> moreLikeThis(@Nullable LLSnapshot snapshot,
			QueryParams queryParams,
			@Nullable String keyFieldName,
			Multimap<String, String> mltDocumentFields) {
		return luceneIndicesFlux.flatMap(luceneIndex -> luceneIndex.moreLikeThis(snapshot,
				queryParams,
				keyFieldName,
				mltDocumentFields
		));
	}

	private Mono<Buckets> mergeShards(List<Buckets> shards) {
		return Mono.fromCallable(() -> {
			List<DoubleArrayList> seriesValues = new ArrayList<>();
			DoubleArrayList totals = new DoubleArrayList(shards.get(0).totals());

			for (Buckets shard : shards) {
				if (seriesValues.isEmpty()) {
					seriesValues.addAll(shard.seriesValues());
				} else {
					for (int serieIndex = 0; serieIndex < seriesValues.size(); serieIndex++) {
						DoubleArrayList mergedSerieValues = seriesValues.get(serieIndex);
						for (int dataIndex = 0; dataIndex < mergedSerieValues.size(); dataIndex++) {
							mergedSerieValues.set(dataIndex, mergedSerieValues.getDouble(dataIndex)
									+ shard.seriesValues().get(serieIndex).getDouble(dataIndex)
							);
						}
					}
				}
				for (int i = 0; i < totals.size(); i++) {
					totals.set(i, totals.getDouble(i) + shard.totals().getDouble(i));
				}
			}
			return new Buckets(seriesValues, totals);
		});
	}

	@Override
	public Flux<LLSearchResultShard> search(@Nullable LLSnapshot snapshot,
			QueryParams queryParams,
			@Nullable String keyFieldName) {
		return luceneIndicesFlux.flatMap(luceneIndex -> luceneIndex.search(snapshot,
				queryParams,
				keyFieldName
		));
	}

	@Override
	public Mono<Buckets> computeBuckets(@Nullable LLSnapshot snapshot,
			@NotNull List<Query> queries,
			@Nullable Query normalizationQuery,
			BucketParams bucketParams) {
		return luceneIndicesFlux.flatMap(luceneIndex -> luceneIndex.computeBuckets(snapshot,
				queries,
				normalizationQuery,
				bucketParams
		)).collectList().flatMap(this::mergeShards);
	}

	@Override
	public Mono<TotalHitsCount> count(@Nullable LLSnapshot snapshot, Query query) {
		return LLLuceneIndex.super.count(snapshot, query);
	}

	@Override
	public boolean isLowMemoryMode() {
		return luceneOptions.lowMemory();
	}

	@Override
	public Mono<Void> close() {
		return luceneIndicesFlux.flatMap(LLLuceneIndex::close).then();
	}

	@Override
	public Mono<Void> flush() {
		return luceneIndicesFlux.flatMap(LLLuceneIndex::flush).then();
	}

	@Override
	public Mono<Void> refresh(boolean force) {
		return luceneIndicesFlux.flatMap(index -> index.refresh(force)).then();
	}

	@Override
	public Mono<LLSnapshot> takeSnapshot() {
		return Mono
				// Generate next snapshot index
				.fromCallable(nextSnapshotNumber::getAndIncrement)
				.flatMap(snapshotIndex -> luceneIndicesFlux
						.flatMapSequential(LLLuceneIndex::takeSnapshot)
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
				.flatMap(tuple -> {
					int index = (int) (long) tuple.getT1();
					LLSnapshot instanceSnapshot = tuple.getT2();
					return luceneIndicesSet.get(index).releaseSnapshot(instanceSnapshot);
				})
				.then();
	}
}
