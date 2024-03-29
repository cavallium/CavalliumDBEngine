package it.cavallium.dbengine.client;

import static it.cavallium.dbengine.utils.StreamUtils.LUCENE_POOL;
import static it.cavallium.dbengine.utils.StreamUtils.collectOn;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toList;

import it.cavallium.dbengine.client.query.ClientQueryParams;
import it.cavallium.dbengine.client.query.current.data.Query;
import it.cavallium.dbengine.client.query.current.data.TotalHitsCount;
import it.cavallium.dbengine.database.LLKeyScore;
import it.cavallium.dbengine.database.LLLuceneIndex;
import it.cavallium.dbengine.database.LLSearchResultShard;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.LLTerm;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.lucene.LuceneUtils;
import it.cavallium.dbengine.lucene.collector.Buckets;
import it.cavallium.dbengine.lucene.searcher.BucketParams;
import it.cavallium.dbengine.utils.StreamUtils;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Stream;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class LuceneIndexImpl<T, U> implements LuceneIndex<T, U> {

	private static final Duration MAX_COUNT_TIME = Duration.ofSeconds(30);
	private final LLLuceneIndex luceneIndex;
	private final Indicizer<T,U> indicizer;

	public LuceneIndexImpl(LLLuceneIndex luceneIndex, Indicizer<T, U> indicizer) {
		this.luceneIndex = luceneIndex;
		this.indicizer = indicizer;
	}

	private LLSnapshot resolveSnapshot(CompositeSnapshot snapshot) {
		if (snapshot == null) {
			return null;
		} else {
			return snapshot.getSnapshot(luceneIndex);
		}
	}

	@Override
	public void addDocument(T key, U value) {
		luceneIndex.addDocument(indicizer.toIndex(key), indicizer.toDocument(key, value));
	}

	@Override
	public long addDocuments(boolean atomic, Stream<Entry<T, U>> entries) {
		return luceneIndex.addDocuments(atomic, entries.map(entry ->
				Map.entry(indicizer.toIndex(entry.getKey()), indicizer.toDocument(entry.getKey(), entry.getValue()))));
	}

	@Override
	public void deleteDocument(T key) {
		LLTerm id = indicizer.toIndex(key);
		luceneIndex.deleteDocument(id);
	}

	@Override
	public void updateDocument(T key, @NotNull U value) {
		luceneIndex.update(indicizer.toIndex(key), indicizer.toIndexRequest(key, value));
	}

	@Override
	public long updateDocuments(Stream<Entry<T, U>> entries) {
		return luceneIndex.updateDocuments(entries.map(entry ->
				Map.entry(indicizer.toIndex(entry.getKey()), indicizer.toDocument(entry.getKey(), entry.getValue()))));
	}

	@Override
	public void deleteAll() {
		luceneIndex.deleteAll();
	}

	@Override
	public Hits<HitKey<T>> moreLikeThis(ClientQueryParams queryParams,
			T key,
			U mltDocumentValue) {
		var mltDocumentFields
				= indicizer.getMoreLikeThisDocumentFields(key, mltDocumentValue);

		return collectOn(LUCENE_POOL, luceneIndex.moreLikeThis(resolveSnapshot(queryParams.snapshot()),
						queryParams.toQueryParams(),
						indicizer.getKeyFieldName(),
						mltDocumentFields),
				collectingAndThen(toList(), toHitsCollector(queryParams)));
	}

	@Override
	public Hits<HitKey<T>> search(ClientQueryParams queryParams) {
		return collectOn(LUCENE_POOL, luceneIndex.search(resolveSnapshot(queryParams.snapshot()),
						queryParams.toQueryParams(),
						indicizer.getKeyFieldName()),
				collectingAndThen(toList(), toHitsCollector(queryParams)));
	}

	@Override
	public Buckets computeBuckets(@Nullable CompositeSnapshot snapshot,
			@NotNull List<Query> query,
			@Nullable Query normalizationQuery,
			BucketParams bucketParams) {
		return luceneIndex.computeBuckets(resolveSnapshot(snapshot), query, normalizationQuery, bucketParams);
	}

	private Hits<HitKey<T>> mapResults(LLSearchResultShard llSearchResult) {
		List<HitKey<T>> scoresWithKeys = LLUtils.mapList(llSearchResult.results(),
				hit -> new HitKey<>(indicizer.getKey(hit.key()), hit.score())
		);
		return new Hits<>(scoresWithKeys, llSearchResult.totalHitsCount());
	}

	@Override
	public TotalHitsCount count(@Nullable CompositeSnapshot snapshot, Query query) {
		return luceneIndex.count(resolveSnapshot(snapshot), query, MAX_COUNT_TIME);
	}

	@Override
	public boolean isLowMemoryMode() {
		return luceneIndex.isLowMemoryMode();
	}

	@Override
	public void close() {
		luceneIndex.close();
	}

	/**
	 * Flush writes to disk
	 */
	@Override
	public void flush() {
		luceneIndex.flush();
	}

	@Override
	public void waitForMerges() {
		luceneIndex.waitForMerges();
	}

	@Override
	public void waitForLastMerges() {
		luceneIndex.waitForLastMerges();
	}

	/**
	 * Refresh index searcher
	 */
	@Override
	public void refresh(boolean force) {
		luceneIndex.refresh(force);
	}

	@Override
	public LLSnapshot takeSnapshot() {
		return luceneIndex.takeSnapshot();
	}

	@Override
	public void releaseSnapshot(LLSnapshot snapshot) {
		luceneIndex.releaseSnapshot(snapshot);
	}

	private Function<List<LLSearchResultShard>, Hits<HitKey<T>>> toHitsCollector(ClientQueryParams queryParams) {
		return (List<LLSearchResultShard> results) -> resultsToHits(mergeResults(queryParams, results));
	}

	private Hits<HitKey<T>> resultsToHits(LLSearchResultShard resultShard) {
		if (resultShard != null) {
			return mapResults(resultShard);
		} else {
			return Hits.empty();
		}
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Nullable
	private static LLSearchResultShard mergeResults(ClientQueryParams queryParams, List<LLSearchResultShard> shards) {
		if (shards.size() == 0) {
			return null;
		} else if (shards.size() == 1) {
			return shards.get(0);
		}
		TotalHitsCount count = null;
		ObjectArrayList<Stream<LLKeyScore>> results = new ObjectArrayList<>(shards.size());
		var maxLimit = queryParams.offset() + queryParams.limit();
		for (LLSearchResultShard shard : shards) {
			if (count == null) {
				count = shard.totalHitsCount();
			} else {
				count = LuceneUtils.sum(count, shard.totalHitsCount());
			}
			results.add(shard.results().stream().limit(maxLimit));
		}
		Objects.requireNonNull(count);
		Stream<LLKeyScore> resultsFlux;
		if (results.size() == 0) {
			resultsFlux = Stream.empty();
		} else if (results.size() == 1) {
			resultsFlux = results.get(0);
		} else {
			resultsFlux = results.stream().flatMap(Function.identity()).limit(maxLimit);
		}
		return new LLSearchResultShard(StreamUtils.toList(resultsFlux), count);
	}

}
