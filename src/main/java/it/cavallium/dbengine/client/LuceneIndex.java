package it.cavallium.dbengine.client;

import it.cavallium.dbengine.database.LLLuceneIndex;
import it.cavallium.dbengine.database.LLScoreMode;
import it.cavallium.dbengine.database.LLSearchResult;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.LLSnapshottable;
import it.cavallium.dbengine.database.LLSort;
import it.cavallium.dbengine.database.LLTerm;
import it.cavallium.dbengine.lucene.LuceneUtils;
import it.cavallium.dbengine.database.collections.Joiner.ValueGetter;
import it.cavallium.dbengine.lucene.serializer.Query;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

public class LuceneIndex<T, U> implements LLSnapshottable {

	private final LLLuceneIndex luceneIndex;
	private final Indicizer<T,U> indicizer;

	public LuceneIndex(LLLuceneIndex luceneIndex, Indicizer<T, U> indicizer) {
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

	public Mono<Void> addDocument(T key, U value) {
		return indicizer
				.toDocument(key, value)
				.flatMap(doc -> luceneIndex.addDocument(indicizer.toIndex(key), doc));
	}

	public Mono<Void> addDocuments(Flux<Entry<T, U>> entries) {
		return luceneIndex
				.addDocuments(entries
						.flatMap(entry -> indicizer
								.toDocument(entry.getKey(), entry.getValue())
								.map(doc -> Map.entry(indicizer.toIndex(entry.getKey()), doc)))
						.groupBy(Entry::getKey, Entry::getValue)
				);
	}

	public Mono<Void> deleteDocument(T key) {
		LLTerm id = indicizer.toIndex(key);
		return luceneIndex.deleteDocument(id);
	}

	public Mono<Void> updateDocument(T key, U value) {
		return indicizer
				.toDocument(key, value)
				.flatMap(doc -> luceneIndex.updateDocument(indicizer.toIndex(key), doc));
	}

	public Mono<Void> updateDocuments(Flux<Entry<T, U>> entries) {
		return luceneIndex
				.updateDocuments(entries
						.flatMap(entry -> indicizer
								.toDocument(entry.getKey(), entry.getValue())
								.map(doc -> Map.entry(indicizer.toIndex(entry.getKey()), doc)))
						.groupBy(Entry::getKey, Entry::getValue)
				);
	}

	public Mono<Void> deleteAll() {
		return luceneIndex.deleteAll();
	}

	private SearchResultKeys<T> transformLuceneResult(LLSearchResult llSearchResult,
			@Nullable MultiSort<SearchResultKey<T>> sort,
			@Nullable Integer limit) {
		var mappedKeys = llSearchResult
				.results()
				.map(flux -> flux.map(item -> new SearchResultKey<>(indicizer.getKey(item.getKey()), item.getScore())));
		var sortedKeys = LuceneUtils.mergeStream(mappedKeys, sort, limit);
		return new SearchResultKeys<>(llSearchResult.totalHitsCount(), sortedKeys);
	}

	private SearchResult<T, U> transformLuceneResultWithValues(LLSearchResult llSearchResult,
			@Nullable MultiSort<SearchResultItem<T, U>> sort,
			@Nullable Integer limit,
			ValueGetter<T, U> valueGetter) {
		var mappedKeys = llSearchResult
				.results()
				.map(flux -> flux.flatMapSequential(item -> {
					var key = indicizer.getKey(item.getKey());
					return valueGetter.get(key).map(value -> new SearchResultItem<>(key, value, item.getScore()));
				}));
		var sortedKeys = LuceneUtils.mergeStream(mappedKeys, sort, limit);
		return new SearchResult<>(llSearchResult.totalHitsCount(), sortedKeys);
	}

	/**
	 *
	 * @param limit the limit is valid for each lucene instance.
	 *               If you have 15 instances, the number of elements returned
	 *               can be at most <code>limit * 15</code>
	 * @return the collection has one or more flux
	 */
	public Mono<SearchResultKeys<T>> moreLikeThis(@Nullable CompositeSnapshot snapshot,
			T key,
			U mltDocumentValue,
			int limit) {
		Flux<Tuple2<String, Set<String>>> mltDocumentFields
				= indicizer.getMoreLikeThisDocumentFields(key, mltDocumentValue);
		return luceneIndex
				.moreLikeThis(resolveSnapshot(snapshot), mltDocumentFields, limit, indicizer.getKeyFieldName())
				.map(llSearchResult -> this.transformLuceneResult(llSearchResult, MultiSort.topScore(), limit));

	}

	/**
	 *
	 * @param limit the limit is valid for each lucene instance.
	 *               If you have 15 instances, the number of elements returned
	 *               can be at most <code>limit * 15</code>
	 * @return the collection has one or more flux
	 */
	public Mono<SearchResult<T, U>> moreLikeThisWithValues(@Nullable CompositeSnapshot snapshot,
			T key,
			U mltDocumentValue,
			int limit,
			ValueGetter<T, U> valueGetter) {
		Flux<Tuple2<String, Set<String>>> mltDocumentFields
				= indicizer.getMoreLikeThisDocumentFields(key, mltDocumentValue);
		return luceneIndex
				.moreLikeThis(resolveSnapshot(snapshot), mltDocumentFields, limit, indicizer.getKeyFieldName())
				.map(llSearchResult ->
						this.transformLuceneResultWithValues(llSearchResult, MultiSort.topScoreWithValues(), limit, valueGetter));
	}

	/**
	 *
	 * @param limit the limit is valid for each lucene instance.
	 *               If you have 15 instances, the number of elements returned
	 *               can be at most <code>limit * 15</code>
	 * @return the collection has one or more flux
	 */
	public Mono<SearchResultKeys<T>> search(@Nullable CompositeSnapshot snapshot,
			Query query,
			int limit,
			@Nullable MultiSort<SearchResultKey<T>> sort,
			LLScoreMode scoreMode) {
		LLSort querySort = sort != null ? sort.getQuerySort() : null;
		return luceneIndex
				.search(resolveSnapshot(snapshot), query, limit, querySort, scoreMode, indicizer.getKeyFieldName())
				.map(llSearchResult -> this.transformLuceneResult(llSearchResult, sort, limit));
	}

	/**
	 *
	 * @param limit the limit is valid for each lucene instance.
	 *               If you have 15 instances, the number of elements returned
	 *               can be at most <code>limit * 15</code>
	 * @return the collection has one or more flux
	 */
	public Mono<SearchResult<T, U>> searchWithValues(@Nullable CompositeSnapshot snapshot,
			Query query,
			int limit,
			@Nullable MultiSort<SearchResultItem<T, U>> sort,
			LLScoreMode scoreMode,
			ValueGetter<T, U> valueGetter) {
		LLSort querySort = sort != null ? sort.getQuerySort() : null;
		return luceneIndex
				.search(resolveSnapshot(snapshot), query, limit, querySort, scoreMode, indicizer.getKeyFieldName())
				.map(llSearchResult -> this.transformLuceneResultWithValues(llSearchResult, sort, limit, valueGetter));
	}

	public Mono<Long> count(@Nullable CompositeSnapshot snapshot, Query query) {
		return this.search(snapshot, query, 0, null, null)
				.flatMap(SearchResultKeys::totalHitsCount)
				.single();
	}

	public boolean isLowMemoryMode() {
		return luceneIndex.isLowMemoryMode();
	}

	public Mono<Void> close() {
		return luceneIndex.close();
	}

	/**
	 * Flush writes to disk
	 */
	public Mono<Void> flush() {
		return luceneIndex.flush();
	}

	/**
	 * Refresh index searcher
	 */
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
