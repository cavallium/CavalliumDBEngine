package it.cavallium.dbengine.database;

import static it.cavallium.dbengine.database.LLUtils.mapList;
import static it.cavallium.dbengine.lucene.LuceneUtils.getLuceneIndexId;
import static it.cavallium.dbengine.utils.StreamUtils.LUCENE_POOL;
import static it.cavallium.dbengine.utils.StreamUtils.collectOn;
import static it.cavallium.dbengine.utils.StreamUtils.executing;
import static it.cavallium.dbengine.utils.StreamUtils.fastListing;
import static it.cavallium.dbengine.utils.StreamUtils.fastReducing;
import static it.cavallium.dbengine.utils.StreamUtils.fastSummingLong;
import static it.cavallium.dbengine.utils.StreamUtils.partitionByInt;
import static java.util.stream.Collectors.groupingBy;

import com.google.common.collect.Multimap;
import it.cavallium.dbengine.client.IBackuppable;
import it.cavallium.dbengine.client.query.current.data.Query;
import it.cavallium.dbengine.client.query.current.data.QueryParams;
import it.cavallium.dbengine.lucene.LuceneHacks;
import it.cavallium.dbengine.lucene.collector.Buckets;
import it.cavallium.dbengine.lucene.searcher.BucketParams;
import it.cavallium.dbengine.rpc.current.data.IndicizerAnalyzers;
import it.cavallium.dbengine.rpc.current.data.IndicizerSimilarities;
import it.cavallium.dbengine.rpc.current.data.LuceneIndexStructure;
import it.cavallium.dbengine.rpc.current.data.LuceneOptions;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

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
	}

	@Override
	public String getLuceneIndexName() {
		return clusterName;
	}

	private LLLuceneIndex getLuceneIndex(LLTerm id) {
		return luceneIndicesById[getLuceneIndexId(id, totalShards)];
	}

	@Override
	public void addDocument(LLTerm id, LLUpdateDocument doc) {
		getLuceneIndex(id).addDocument(id, doc);
	}

	@Override
	public long addDocuments(boolean atomic, Stream<Entry<LLTerm, LLUpdateDocument>> documents) {
		return collectOn(LUCENE_POOL,
				partitionByInt(term -> getLuceneIndexId(term.getKey(), totalShards), documents)
						.map(entry -> luceneIndicesById[entry.key()].addDocuments(atomic, entry.values().stream())),
				fastSummingLong()
		);
	}

	@Override
	public void deleteDocument(LLTerm id) {
		getLuceneIndex(id).deleteDocument(id);
	}

	@Override
	public void update(LLTerm id, LLIndexRequest request) {
		getLuceneIndex(id).update(id, request);
	}

	@Override
	public long updateDocuments(Stream<Entry<LLTerm, LLUpdateDocument>> documents) {
		return collectOn(LUCENE_POOL,
				partitionByInt(term -> getLuceneIndexId(term.getKey(), totalShards), documents)
						.map(entry -> luceneIndicesById[entry.key()].updateDocuments(entry.values().stream())),
				fastSummingLong()
		);
	}

	@Override
	public void deleteAll() {
		luceneIndicesSet.forEach(LLLuceneIndex::deleteAll);
	}

	@Override
	public Stream<LLSearchResultShard> moreLikeThis(@Nullable LLSnapshot snapshot,
			QueryParams queryParams,
			@Nullable String keyFieldName,
			Multimap<String, String> mltDocumentFields) {
		return luceneIndicesSet.stream().flatMap(luceneIndex -> luceneIndex.moreLikeThis(snapshot,
				queryParams,
				keyFieldName,
				mltDocumentFields
		));
	}

	private Buckets mergeShards(List<Buckets> shards) {
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
	}

	@Override
	public Stream<LLSearchResultShard> search(@Nullable LLSnapshot snapshot,
			QueryParams queryParams,
			@Nullable String keyFieldName) {
		return luceneIndicesSet.stream().flatMap(luceneIndex -> luceneIndex.search(snapshot,
				queryParams,
				keyFieldName
		));
	}

	@Override
	public Buckets computeBuckets(@Nullable LLSnapshot snapshot,
			@NotNull List<Query> queries,
			@Nullable Query normalizationQuery,
			BucketParams bucketParams) {
		return mergeShards(mapList(luceneIndicesSet, luceneIndex -> luceneIndex.computeBuckets(snapshot,
				queries,
				normalizationQuery,
				bucketParams
		)));
	}

	@Override
	public boolean isLowMemoryMode() {
		return luceneOptions.lowMemory();
	}

	@Override
	public void close() {
		collectOn(LUCENE_POOL, luceneIndicesSet.stream(), executing(LLLuceneIndex::close));
	}

	@Override
	public void flush() {
		collectOn(LUCENE_POOL, luceneIndicesSet.stream(), executing(LLLuceneIndex::flush));
	}

	@Override
	public void waitForMerges() {
		collectOn(LUCENE_POOL, luceneIndicesSet.stream(), executing(LLLuceneIndex::waitForMerges));
	}

	@Override
	public void waitForLastMerges() {
		collectOn(LUCENE_POOL, luceneIndicesSet.stream(), executing(LLLuceneIndex::waitForLastMerges));
	}

	@Override
	public void refresh(boolean force) {
		collectOn(LUCENE_POOL, luceneIndicesSet.stream(), executing(index -> index.refresh(force)));
	}

	@Override
	public LLSnapshot takeSnapshot() {
		// Generate next snapshot index
		var snapshotIndex = nextSnapshotNumber.getAndIncrement();
		var snapshot = collectOn(LUCENE_POOL, luceneIndicesSet.stream().map(LLSnapshottable::takeSnapshot), fastListing());
		registeredSnapshots.put(snapshotIndex, snapshot);
		return new LLSnapshot(snapshotIndex);
	}

	@Override
	public void releaseSnapshot(LLSnapshot snapshot) {
		var list = registeredSnapshots.remove(snapshot.getSequenceNumber());
		for (int shardIndex = 0; shardIndex < list.size(); shardIndex++) {
			var luceneIndex = luceneIndicesSet.get(shardIndex);
			LLSnapshot instanceSnapshot = list.get(shardIndex);
			luceneIndex.releaseSnapshot(instanceSnapshot);
		}
	}

	@Override
	public void pauseForBackup() {
		collectOn(LUCENE_POOL, luceneIndicesSet.stream(), executing(LLLuceneIndex::pauseForBackup));
	}

	@Override
	public void resumeAfterBackup() {
		collectOn(LUCENE_POOL, luceneIndicesSet.stream(), executing(LLLuceneIndex::resumeAfterBackup));
	}

	@Override
	public boolean isPaused() {
		return this.luceneIndicesSet.stream().anyMatch(IBackuppable::isPaused);
	}
}
