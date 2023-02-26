package it.cavallium.dbengine.database.disk;

import static it.cavallium.dbengine.lucene.LuceneUtils.getLuceneIndexId;
import static it.cavallium.dbengine.utils.StreamUtils.LUCENE_SCHEDULER;
import static it.cavallium.dbengine.utils.StreamUtils.collect;
import static it.cavallium.dbengine.utils.StreamUtils.collectOn;
import static it.cavallium.dbengine.utils.StreamUtils.executing;
import static it.cavallium.dbengine.utils.StreamUtils.fastListing;
import static it.cavallium.dbengine.utils.StreamUtils.fastReducing;
import static it.cavallium.dbengine.utils.StreamUtils.fastSummingLong;
import static it.cavallium.dbengine.utils.StreamUtils.partitionByInt;
import static java.util.stream.Collectors.groupingBy;

import com.google.common.collect.Multimap;
import com.google.common.collect.Streams;
import io.micrometer.core.instrument.MeterRegistry;
import it.cavallium.dbengine.client.IBackuppable;
import it.cavallium.dbengine.client.query.QueryParser;
import it.cavallium.dbengine.client.query.current.data.Query;
import it.cavallium.dbengine.client.query.current.data.QueryParams;
import it.cavallium.dbengine.client.query.current.data.TotalHitsCount;
import it.cavallium.dbengine.database.LLIndexRequest;
import it.cavallium.dbengine.database.LLLuceneIndex;
import it.cavallium.dbengine.database.LLSearchResultShard;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.LLSnapshottable;
import it.cavallium.dbengine.database.LLTerm;
import it.cavallium.dbengine.database.LLUpdateDocument;
import it.cavallium.dbengine.database.SafeCloseable;
import it.cavallium.dbengine.lucene.LuceneCloseable;
import it.cavallium.dbengine.lucene.LuceneHacks;
import it.cavallium.dbengine.lucene.LuceneUtils;
import it.cavallium.dbengine.lucene.collector.Buckets;
import it.cavallium.dbengine.lucene.mlt.MoreLikeThisTransformer;
import it.cavallium.dbengine.lucene.searcher.AdaptiveMultiSearcher;
import it.cavallium.dbengine.lucene.searcher.BucketParams;
import it.cavallium.dbengine.lucene.searcher.DecimalBucketMultiSearcher;
import it.cavallium.dbengine.lucene.searcher.GlobalQueryRewrite;
import it.cavallium.dbengine.lucene.searcher.LocalQueryParams;
import it.cavallium.dbengine.lucene.searcher.LuceneSearchResult;
import it.cavallium.dbengine.lucene.searcher.MultiSearcher;
import it.cavallium.dbengine.rpc.current.data.IndicizerAnalyzers;
import it.cavallium.dbengine.rpc.current.data.IndicizerSimilarities;
import it.cavallium.dbengine.rpc.current.data.LuceneOptions;
import it.cavallium.dbengine.utils.DBException;
import it.cavallium.dbengine.utils.SimpleResource;
import it.cavallium.dbengine.utils.StreamUtils;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntList;
import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.search.similarities.PerFieldSimilarityWrapper;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class LLLocalMultiLuceneIndex extends SimpleResource implements LLLuceneIndex, LuceneCloseable {

	private static final Logger LOG = LogManager.getLogger(LLLuceneIndex.class);
	private static final boolean BYPASS_GROUPBY_BUG = Boolean.parseBoolean(System.getProperty(
			"it.cavallium.dbengine.bypassGroupByBug",
			"false"
	));

	private final String clusterName;
	private final boolean lowMemory;
	private final MeterRegistry meterRegistry;
	private final ConcurrentHashMap<Long, List<LLSnapshot>> registeredSnapshots = new ConcurrentHashMap<>();
	private final AtomicLong nextSnapshotNumber = new AtomicLong(1);
	private final LLLocalLuceneIndex[] luceneIndicesById;
	private final List<LLLocalLuceneIndex> luceneIndicesSet;
	private final int totalShards;
	private final PerFieldAnalyzerWrapper luceneAnalyzer;
	private final PerFieldSimilarityWrapper luceneSimilarity;

	private final MultiSearcher multiSearcher;
	private final DecimalBucketMultiSearcher decimalBucketMultiSearcher = new DecimalBucketMultiSearcher();

	public LLLocalMultiLuceneIndex(MeterRegistry meterRegistry,
			String clusterName,
			IntList activeShards,
			int totalShards,
			IndicizerAnalyzers indicizerAnalyzers,
			IndicizerSimilarities indicizerSimilarities,
			LuceneOptions luceneOptions,
			@Nullable LuceneHacks luceneHacks) {

		if (totalShards <= 1 || totalShards > 100) {
			throw new DBException("Unsupported instances count: " + totalShards);
		}

		this.meterRegistry = meterRegistry;
		LLLocalLuceneIndex[] luceneIndices = new LLLocalLuceneIndex[totalShards];
		for (int i = 0; i < totalShards; i++) {
			if (!activeShards.contains(i)) {
				continue;
			}
			luceneIndices[i] = new LLLocalLuceneIndex(meterRegistry,
					clusterName,
					i,
					indicizerAnalyzers,
					indicizerSimilarities,
					luceneOptions,
					luceneHacks
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
		this.luceneAnalyzer = LuceneUtils.toPerFieldAnalyzerWrapper(indicizerAnalyzers);
		this.luceneSimilarity = LuceneUtils.toPerFieldSimilarityWrapper(indicizerSimilarities);
		this.lowMemory = luceneOptions.lowMemory();

		var maxInMemoryResultEntries = luceneOptions.maxInMemoryResultEntries();
		if (luceneHacks != null && luceneHacks.customMultiSearcher() != null) {
			multiSearcher = luceneHacks.customMultiSearcher().get();
		} else {
			multiSearcher = new AdaptiveMultiSearcher(maxInMemoryResultEntries);
		}
	}

	private LLLocalLuceneIndex getLuceneIndex(LLTerm id) {
		return Objects.requireNonNull(luceneIndicesById[LuceneUtils.getLuceneIndexId(id, totalShards)]);
	}

	@Override
	public String getLuceneIndexName() {
		return clusterName;
	}

	private LLIndexSearchers getIndexSearchers(LLSnapshot snapshot) {
		// Resolve the snapshot of each shard
		return LLIndexSearchers.of(StreamUtils.toListOn(StreamUtils.LUCENE_SCHEDULER,
				Streams.mapWithIndex(this.luceneIndicesSet.stream(), (luceneIndex, index) -> {
					var subSnapshot = resolveSnapshot(snapshot, (int) index);
					return luceneIndex.retrieveSearcher(subSnapshot);
				})
		));
	}

	@Override
	public void addDocument(LLTerm id, LLUpdateDocument doc) {
		getLuceneIndex(id).addDocument(id, doc);
	}

	@Override
	public long addDocuments(boolean atomic, Stream<Entry<LLTerm, LLUpdateDocument>> documents) {
		return collectOn(LUCENE_SCHEDULER,
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
		return collectOn(LUCENE_SCHEDULER,
				partitionByInt(term -> getLuceneIndexId(term.getKey(), totalShards), documents)
						.map(entry -> luceneIndicesById[entry.key()].updateDocuments(entry.values().stream())),
				fastSummingLong()
		);
	}

	@Override
	public void deleteAll() {
		luceneIndicesSet.forEach(LLLuceneIndex::deleteAll);
	}

	private LLSnapshot resolveSnapshot(LLSnapshot multiSnapshot, int instanceId) {
		if (multiSnapshot != null) {
			return registeredSnapshots.get(multiSnapshot.getSequenceNumber()).get(instanceId);
		} else {
			return null;
		}
	}

	@Override
	public Stream<LLSearchResultShard> moreLikeThis(@Nullable LLSnapshot snapshot,
			QueryParams queryParams,
			String keyFieldName,
			Multimap<String, String> mltDocumentFields) {
		LocalQueryParams localQueryParams = LuceneUtils.toLocalQueryParams(queryParams, luceneAnalyzer);
		try (var searchers = this.getIndexSearchers(snapshot)) {
			var transformer = new MoreLikeThisTransformer(mltDocumentFields, luceneAnalyzer, luceneSimilarity);

			// Collect all the shards results into a single global result
			LuceneSearchResult result = multiSearcher.collectMulti(searchers,
					localQueryParams,
					keyFieldName,
					transformer,
					Function.identity()
			);

			// Transform the result type
			return Stream.of(new LLSearchResultShard(result.results(), result.totalHitsCount()));
		}
	}

	@Override
	public Stream<LLSearchResultShard> search(@Nullable LLSnapshot snapshot,
			QueryParams queryParams,
			@Nullable String keyFieldName) {
		LuceneSearchResult result = searchInternal(snapshot, queryParams, keyFieldName);
		// Transform the result type
		var shard = new LLSearchResultShard(result.results(), result.totalHitsCount());
		return Stream.of(shard);
	}

	private LuceneSearchResult searchInternal(@Nullable LLSnapshot snapshot,
			QueryParams queryParams,
			@Nullable String keyFieldName) {
		LocalQueryParams localQueryParams = LuceneUtils.toLocalQueryParams(queryParams, luceneAnalyzer);
		try (var searchers = getIndexSearchers(snapshot)) {

			// Collect all the shards results into a single global result
			return multiSearcher.collectMulti(searchers,
					localQueryParams,
					keyFieldName,
					GlobalQueryRewrite.NO_REWRITE,
					Function.identity()
			);
		}
	}

	@Override
	public TotalHitsCount count(@Nullable LLSnapshot snapshot, Query query, @Nullable Duration timeout) {
		var params = LuceneUtils.getCountQueryParams(query);
		var result = this.searchInternal(snapshot, params, null);
		return result != null ? result.totalHitsCount() : TotalHitsCount.of(0, true);
	}

	@Override
	public Buckets computeBuckets(@Nullable LLSnapshot snapshot,
			@NotNull List<Query> queries,
			@Nullable Query normalizationQuery,
			BucketParams bucketParams) {
		List<org.apache.lucene.search.Query> localQueries = new ArrayList<>(queries.size());
		for (Query query : queries) {
			localQueries.add(QueryParser.toQuery(query, luceneAnalyzer));
		}
		var localNormalizationQuery = QueryParser.toQuery(normalizationQuery, luceneAnalyzer);
		try (var searchers = getIndexSearchers(snapshot)) {

			// Collect all the shards results into a single global result
			return decimalBucketMultiSearcher.collectMulti(searchers, bucketParams, localQueries, localNormalizationQuery);
		}
	}

	@Override
	protected void onClose() {
		collectOn(StreamUtils.LUCENE_SCHEDULER, luceneIndicesSet.stream(), executing(SafeCloseable::close));
		if (multiSearcher instanceof Closeable closeable) {
			try {
				closeable.close();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	@Override
	public void flush() {
		collectOn(StreamUtils.LUCENE_SCHEDULER, luceneIndicesSet.stream(), executing(LLLuceneIndex::flush));
	}

	@Override
	public void waitForMerges() {
		collectOn(StreamUtils.LUCENE_SCHEDULER, luceneIndicesSet.stream(), executing(LLLuceneIndex::waitForMerges));
	}

	@Override
	public void waitForLastMerges() {
		collectOn(StreamUtils.LUCENE_SCHEDULER, luceneIndicesSet.stream(), executing(LLLuceneIndex::waitForLastMerges));
	}

	@Override
	public void refresh(boolean force) {
		collectOn(StreamUtils.LUCENE_SCHEDULER, luceneIndicesSet.stream(), executing(index -> index.refresh(force)));
	}

	@Override
	public LLSnapshot takeSnapshot() {
		// Generate next snapshot index
		var snapshotIndex = nextSnapshotNumber.getAndIncrement();
		var snapshot = collectOn(StreamUtils.LUCENE_SCHEDULER,
				luceneIndicesSet.stream().map(LLSnapshottable::takeSnapshot),
				fastListing()
		);
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
	public boolean isLowMemoryMode() {
		return lowMemory;
	}

	@Override
	public void pauseForBackup() {
		collectOn(StreamUtils.LUCENE_SCHEDULER, luceneIndicesSet.stream(), executing(LLLuceneIndex::pauseForBackup));
	}

	@Override
	public void resumeAfterBackup() {
		collectOn(StreamUtils.LUCENE_SCHEDULER, luceneIndicesSet.stream(), executing(LLLuceneIndex::resumeAfterBackup));
	}

	@Override
	public boolean isPaused() {
		return this.luceneIndicesSet.stream().anyMatch(IBackuppable::isPaused);
	}
}
