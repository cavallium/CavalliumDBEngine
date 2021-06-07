package it.cavallium.dbengine.client;

import it.cavallium.dbengine.client.query.ClientQueryParams;
import it.cavallium.dbengine.client.query.ClientQueryParamsBuilder;
import it.cavallium.dbengine.client.query.current.data.Query;
import it.cavallium.dbengine.client.query.current.data.QueryParams;
import it.cavallium.dbengine.database.LLLuceneIndex;
import it.cavallium.dbengine.database.LLScoreMode;
import it.cavallium.dbengine.database.LLSearchResult;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.LLTerm;
import it.cavallium.dbengine.database.collections.Joiner.ValueGetter;
import it.cavallium.dbengine.lucene.LuceneUtils;
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
				.toDocument(key, value)
				.flatMap(doc -> luceneIndex.updateDocument(indicizer.toIndex(key), doc));
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

	private static QueryParams fixOffset(LLLuceneIndex luceneIndex, QueryParams queryParams) {
		if (luceneIndex.supportsOffset()) {
			return queryParams;
		} else {
			return queryParams.setOffset(0);
		}
	}

	private static long fixTransformOffset(LLLuceneIndex luceneIndex, long offset) {
		if (luceneIndex.supportsOffset()) {
			return 0;
		} else {
			return offset;
		}
	}

	private Mono<SearchResultKeys<T>> transformLuceneResult(LLSearchResult llSearchResult,
			@Nullable MultiSort<SearchResultKey<T>> sort,
			LLScoreMode scoreMode,
			long offset,
			@Nullable Long limit) {
		Flux<SearchResultKeys<T>> mappedKeys = llSearchResult
				.results()
				.map(flux -> new SearchResultKeys<>(flux
						.results()
						.map(signal -> new SearchResultKey<>(indicizer.getKey(signal.getKey()), signal.getScore())),
						flux.totalHitsCount()
				));
		MultiSort<SearchResultKey<T>> finalSort;
		if (scoreMode != LLScoreMode.COMPLETE_NO_SCORES && sort == null) {
			finalSort = MultiSort.topScore();
		} else {
			finalSort = sort;
		}

		MultiSort<SearchResultKey<T>> mappedSort;
		if (finalSort != null) {
			mappedSort = new MultiSort<>(
					finalSort.getQuerySort(),
					(signal1, signal2) -> finalSort.getResultSort().compare((signal1), signal2)
			);
		} else {
			mappedSort = null;
		}
		return LuceneUtils.mergeSignalStreamKeys(mappedKeys, mappedSort, offset, limit);
	}

	private Mono<SearchResult<T, U>> transformLuceneResultWithValues(LLSearchResult llSearchResult,
			@Nullable MultiSort<SearchResultItem<T, U>> sort,
			LLScoreMode scoreMode,
			long offset,
			@Nullable Long limit,
			ValueGetter<T, U> valueGetter) {
		Flux<SearchResult<T, U>> mappedKeys = llSearchResult
				.results()
				.map(flux -> new SearchResult<>(flux.results().flatMapSequential(signal -> {
						var key = indicizer.getKey(signal.getKey());
						return valueGetter
								.get(key)
								.map(value -> new SearchResultItem<>(key, value, signal.getScore()));
				}), flux.totalHitsCount()));
		MultiSort<SearchResultItem<T, U>> finalSort;
		if (scoreMode != LLScoreMode.COMPLETE_NO_SCORES && sort == null) {
			finalSort = MultiSort.topScoreWithValues();
		} else {
			finalSort = sort;
		}

		MultiSort<SearchResultItem<T, U>> mappedSort;
		if (finalSort != null) {
			mappedSort = new MultiSort<>(
					finalSort.getQuerySort(),
					(signal1, signal2) -> finalSort.getResultSort().compare((signal1), signal2)
			);
		} else {
			mappedSort = null;
		}
		return LuceneUtils.mergeSignalStreamItems(mappedKeys, mappedSort, offset, limit);
	}

	/**
	 *
	 * @param queryParams the limit is valid for each lucene instance.
	 *               If you have 15 instances, the number of elements returned
	 *               can be at most <code>limit * 15</code>
	 * @return the collection has one or more flux
	 */
	@Override
	public Mono<SearchResultKeys<T>> moreLikeThis(ClientQueryParams<SearchResultKey<T>> queryParams,
			T key,
			U mltDocumentValue) {
		Flux<Tuple2<String, Set<String>>> mltDocumentFields
				= indicizer.getMoreLikeThisDocumentFields(key, mltDocumentValue);
		return luceneIndex
				.moreLikeThis(resolveSnapshot(queryParams.snapshot()), fixOffset(luceneIndex, queryParams.toQueryParams()), indicizer.getKeyFieldName(), mltDocumentFields)
				.flatMap(llSearchResult -> this.transformLuceneResult(llSearchResult,
						queryParams.sort(),
						queryParams.scoreMode(),
						fixTransformOffset(luceneIndex, queryParams.offset()),
						queryParams.limit()
				));

	}


	/**
	 *
	 * @param queryParams the limit is valid for each lucene instance.
	 *               If you have 15 instances, the number of elements returned
	 *               can be at most <code>limit * 15</code>
	 * @return the collection has one or more flux
	 */
	@Override
	public Mono<SearchResult<T, U>> moreLikeThisWithValues(ClientQueryParams<SearchResultItem<T, U>> queryParams,
			T key,
			U mltDocumentValue,
			ValueGetter<T, U> valueGetter) {
		Flux<Tuple2<String, Set<String>>> mltDocumentFields
				= indicizer.getMoreLikeThisDocumentFields(key, mltDocumentValue);
		return luceneIndex
				.moreLikeThis(resolveSnapshot(queryParams.snapshot()),
						fixOffset(luceneIndex, queryParams.toQueryParams()),
						indicizer.getKeyFieldName(),
						mltDocumentFields
				)
				.flatMap(llSearchResult -> this.transformLuceneResultWithValues(llSearchResult,
						queryParams.sort(),
						queryParams.scoreMode(),
						fixTransformOffset(luceneIndex, queryParams.offset()),
						queryParams.limit(),
						valueGetter
				));
	}

	/**
	 *
	 * @param queryParams the limit is valid for each lucene instance.
	 *               If you have 15 instances, the number of elements returned
	 *               can be at most <code>limit * 15</code>
	 * @return the collection has one or more flux
	 */
	@Override
	public Mono<SearchResultKeys<T>> search(ClientQueryParams<SearchResultKey<T>> queryParams) {
		return luceneIndex
				.search(resolveSnapshot(queryParams.snapshot()),
						fixOffset(luceneIndex, queryParams.toQueryParams()),
						indicizer.getKeyFieldName()
				)
				.flatMap(llSearchResult -> this.transformLuceneResult(llSearchResult,
						queryParams.sort(),
						queryParams.scoreMode(),
						fixTransformOffset(luceneIndex, queryParams.offset()),
						queryParams.limit()
				));
	}

	/**
	 *
	 * @param queryParams the limit is valid for each lucene instance.
	 *               If you have 15 instances, the number of elements returned
	 *               can be at most <code>limit * 15</code>
	 * @return the collection has one or more flux
	 */
	@Override
	public Mono<SearchResult<T, U>> searchWithValues(ClientQueryParams<SearchResultItem<T, U>> queryParams,
			ValueGetter<T, U> valueGetter) {
		return luceneIndex
				.search(resolveSnapshot(queryParams.snapshot()), fixOffset(luceneIndex, queryParams.toQueryParams()), indicizer.getKeyFieldName())
				.flatMap(llSearchResult -> this.transformLuceneResultWithValues(llSearchResult,
						queryParams.sort(),
						queryParams.scoreMode(),
						fixTransformOffset(luceneIndex, queryParams.offset()),
						queryParams.limit(),
						valueGetter
				));
	}

	@Override
	public Mono<Long> count(@Nullable CompositeSnapshot snapshot, Query query) {
		return this.search(ClientQueryParams.<SearchResultKey<T>>builder().snapshot(snapshot).query(query).limit(0).build())
				.map(SearchResultKeys::totalHitsCount);
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
	public Mono<Void> refresh() {
		return luceneIndex.refresh();
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
