package it.cavallium.dbengine.client;

import io.net5.buffer.api.Send;
import it.cavallium.dbengine.client.query.ClientQueryParams;
import it.cavallium.dbengine.client.query.current.data.Query;
import it.cavallium.dbengine.client.query.current.data.QueryParams;
import it.cavallium.dbengine.client.query.current.data.TotalHitsCount;
import it.cavallium.dbengine.database.LLLuceneIndex;
import it.cavallium.dbengine.database.LLSearchResultShard;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.LLTerm;
import it.cavallium.dbengine.lucene.collector.Buckets;
import it.cavallium.dbengine.lucene.searcher.BucketParams;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

public class LuceneIndexImpl<T, U> implements LuceneIndex<T, U> {

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
	public Mono<Void> addDocument(T key, U value) {
		return indicizer
				.toDocument(key, value)
				.flatMap(doc -> luceneIndex.addDocument(indicizer.toIndex(key), doc));
	}

	@Override
	public Mono<Void> addDocuments(Flux<Entry<T, U>> entries) {
		return luceneIndex
				.addDocuments(entries
						.flatMap(entry -> indicizer
								.toDocument(entry.getKey(), entry.getValue())
								.map(doc -> Map.entry(indicizer.toIndex(entry.getKey()), doc)))
				);
	}

	@Override
	public Mono<Void> deleteDocument(T key) {
		LLTerm id = indicizer.toIndex(key);
		return luceneIndex.deleteDocument(id);
	}

	@Override
	public Mono<Void> updateDocument(T key, @NotNull U value) {
		return indicizer
				.toIndexRequest(key, value)
				.flatMap(doc -> luceneIndex.update(indicizer.toIndex(key), doc));
	}

	@Override
	public Mono<Void> updateDocuments(Flux<Entry<T, U>> entries) {
		return luceneIndex
				.updateDocuments(entries
						.flatMap(entry -> indicizer
								.toDocument(entry.getKey(), entry.getValue())
								.map(doc -> Map.entry(indicizer.toIndex(entry.getKey()), doc)))
						.collectMap(Entry::getKey, Entry::getValue)
				);
	}

	@Override
	public Mono<Void> deleteAll() {
		return luceneIndex.deleteAll();
	}

	@Override
	public Mono<Hits<HitKey<T>>> moreLikeThis(ClientQueryParams queryParams,
			T key,
			U mltDocumentValue) {
		Flux<Tuple2<String, Set<String>>> mltDocumentFields
				= indicizer.getMoreLikeThisDocumentFields(key, mltDocumentValue);

		return luceneIndex
				.moreLikeThis(resolveSnapshot(queryParams.snapshot()),
						queryParams.toQueryParams(),
						indicizer.getKeyFieldName(),
						mltDocumentFields
				)
				.map(this::mapResults)
				.single();
	}

	@Override
	public Mono<Hits<HitKey<T>>> search(ClientQueryParams queryParams) {
		return luceneIndex
				.search(resolveSnapshot(queryParams.snapshot()),
						queryParams.toQueryParams(),
						indicizer.getKeyFieldName()
				)
				.map(this::mapResults)
				.single();
	}

	@Override
	public Mono<Buckets> computeBuckets(@Nullable CompositeSnapshot snapshot,
			@NotNull List<Query> query,
			@Nullable Query normalizaitonQuery,
			BucketParams bucketParams) {
		return luceneIndex
				.computeBuckets(resolveSnapshot(snapshot),
						query,
						normalizaitonQuery,
						bucketParams
				)
				.single();
	}

	private Hits<HitKey<T>> mapResults(LLSearchResultShard llSearchResult) {
		var scoresWithKeysFlux = llSearchResult
				.results()
				.map(hit -> new HitKey<>(indicizer.getKey(hit.key()), hit.score()));

		return new Hits<>(scoresWithKeysFlux, llSearchResult.totalHitsCount(), llSearchResult::close);
	}

	@Override
	public Mono<TotalHitsCount> count(@Nullable CompositeSnapshot snapshot, Query query) {
		return this
				.search(ClientQueryParams.builder().snapshot(snapshot).query(query).limit(0).build())
				.single()
				.map(searchResultKeys -> {
					try (searchResultKeys) {
						return searchResultKeys.totalHitsCount();
					}
				});
	}

	@Override
	public boolean isLowMemoryMode() {
		return luceneIndex.isLowMemoryMode();
	}

	@Override
	public Mono<Void> close() {
		return luceneIndex.close();
	}

	/**
	 * Flush writes to disk
	 */
	@Override
	public Mono<Void> flush() {
		return luceneIndex.flush();
	}

	/**
	 * Refresh index searcher
	 */
	@Override
	public Mono<Void> refresh(boolean force) {
		return luceneIndex.refresh(force);
	}

	@Override
	public Mono<LLSnapshot> takeSnapshot() {
		return luceneIndex.takeSnapshot();
	}

	@Override
	public Mono<Void> releaseSnapshot(LLSnapshot snapshot) {
		return luceneIndex.releaseSnapshot(snapshot);
	}
}
