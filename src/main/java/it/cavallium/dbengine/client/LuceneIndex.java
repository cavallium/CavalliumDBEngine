package it.cavallium.dbengine.client;

import it.cavallium.dbengine.client.query.ClientQueryParams;
import it.cavallium.dbengine.client.query.current.data.Query;
import it.cavallium.dbengine.database.Delta;
import it.cavallium.dbengine.database.LLSnapshottable;
import it.cavallium.dbengine.database.collections.Joiner.ValueGetter;
import java.util.Map.Entry;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

@SuppressWarnings("unused")
public interface LuceneIndex<T, U> extends LLSnapshottable {

	Mono<Void> addDocument(T key, U value);

	Mono<Void> addDocuments(Flux<Entry<T, U>> entries);

	Mono<Void> deleteDocument(T key);

	Mono<Void> updateDocument(T key, @NotNull U value);

	Mono<Void> updateDocuments(Flux<Entry<T, U>> entries);

	default Mono<Void> updateOrDeleteDocument(T key, @Nullable U value) {
		if (value == null) {
			return deleteDocument(key);
		} else {
			return updateDocument(key, value);
		}
	}

	default Mono<Void> updateOrDeleteDocumentIfModified(T key, @NotNull Delta<U> delta) {
		if (delta.isModified()) {
			return updateOrDeleteDocument(key, delta.current());
		} else {
			return Mono.empty();
		}
	}

	Mono<Void> deleteAll();

	<V> Mono<SearchResultKeys<T>> moreLikeThis(ClientQueryParams<SearchResultKey<T>> queryParams, T key, U mltDocumentValue);

	<V> Mono<SearchResult<T, U>> moreLikeThisWithValues(ClientQueryParams<SearchResultItem<T, U>> queryParams,
			T key,
			U mltDocumentValue,
			ValueGetter<T, U> valueGetter);

	<V> Mono<SearchResultKeys<T>> search(ClientQueryParams<SearchResultKey<T>> queryParams);

	<V> Mono<SearchResult<T, U>> searchWithValues(ClientQueryParams<SearchResultItem<T, U>> queryParams,
			ValueGetter<T, U> valueGetter);

	Mono<Long> count(@Nullable CompositeSnapshot snapshot, Query query);

	boolean isLowMemoryMode();

	Mono<Void> close();

	Mono<Void> flush();

	Mono<Void> refresh();
}
