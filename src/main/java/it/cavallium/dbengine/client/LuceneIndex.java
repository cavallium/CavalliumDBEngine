package it.cavallium.dbengine.client;

import it.cavallium.dbengine.client.query.ClientQueryParams;
import it.cavallium.dbengine.client.query.current.data.Query;
import it.cavallium.dbengine.database.LLSnapshottable;
import it.cavallium.dbengine.database.collections.Joiner.ValueGetter;
import java.util.Map.Entry;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@SuppressWarnings("unused")
public interface LuceneIndex<T, U> extends LLSnapshottable {

	Mono<Void> addDocument(T key, U value);

	Mono<Void> addDocuments(Flux<Entry<T, U>> entries);

	Mono<Void> deleteDocument(T key);

	Mono<Void> updateDocument(T key, U value);

	Mono<Void> updateDocuments(Flux<Entry<T, U>> entries);

	Mono<Void> deleteAll();

	Mono<SearchResultKeys<T>> moreLikeThis(ClientQueryParams<SearchResultKey<T>> queryParams, T key, U mltDocumentValue);

	Mono<SearchResult<T, U>> moreLikeThisWithValues(ClientQueryParams<SearchResultItem<T, U>> queryParams,
			T key,
			U mltDocumentValue,
			ValueGetter<T, U> valueGetter);

	Mono<SearchResultKeys<T>> search(ClientQueryParams<SearchResultKey<T>> queryParams);

	Mono<SearchResult<T, U>> searchWithValues(ClientQueryParams<SearchResultItem<T, U>> queryParams,
			ValueGetter<T, U> valueGetter);

	Mono<Long> count(@Nullable CompositeSnapshot snapshot, Query query);

	boolean isLowMemoryMode();

	Mono<Void> close();

	Mono<Void> flush();

	Mono<Void> refresh();
}
