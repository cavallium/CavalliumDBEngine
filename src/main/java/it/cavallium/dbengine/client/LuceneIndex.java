package it.cavallium.dbengine.client;

import it.cavallium.dbengine.client.query.ClientQueryParams;
import it.cavallium.dbengine.client.query.current.data.Query;
import it.cavallium.dbengine.database.Delta;
import it.cavallium.dbengine.database.LLSnapshottable;
import it.cavallium.dbengine.database.collections.ValueGetter;
import it.cavallium.dbengine.database.collections.ValueTransformer;
import java.util.Map.Entry;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuple3;
import reactor.util.function.Tuples;

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
		return updateOrDeleteDocumentIfModified(key, delta.current(), delta.isModified());
	}

	default Mono<Void> updateOrDeleteDocumentIfModified(T key, @Nullable U currentValue, boolean modified) {
		if (modified) {
			return updateOrDeleteDocument(key, currentValue);
		} else {
			return Mono.empty();
		}
	}

	Mono<Void> deleteAll();

	Mono<SearchResultKeys<T>> moreLikeThis(ClientQueryParams<SearchResultKey<T>> queryParams, T key, U mltDocumentValue);

	default Mono<SearchResult<T, U>> moreLikeThisWithValues(ClientQueryParams<SearchResultItem<T, U>> queryParams,
			T key,
			U mltDocumentValue,
			ValueGetter<T, U> valueGetter) {
		return this.moreLikeThisWithTransformer(queryParams,
				key,
				mltDocumentValue,
				getValueGetterTransformer(valueGetter));
	}

	Mono<SearchResult<T, U>> moreLikeThisWithTransformer(ClientQueryParams<SearchResultItem<T, U>> queryParams,
			T key,
			U mltDocumentValue,
			ValueTransformer<T, U> valueTransformer);

	Mono<SearchResultKeys<T>> search(ClientQueryParams<SearchResultKey<T>> queryParams);

	default Mono<SearchResult<T, U>> searchWithValues(ClientQueryParams<SearchResultItem<T, U>> queryParams,
			ValueGetter<T, U> valueGetter) {
		return this.searchWithTransformer(queryParams,
				getValueGetterTransformer(valueGetter)
		);
	}

	Mono<SearchResult<T, U>> searchWithTransformer(ClientQueryParams<SearchResultItem<T, U>> queryParams,
			ValueTransformer<T, U> valueTransformer);

	Mono<Long> count(@Nullable CompositeSnapshot snapshot, Query query);

	boolean isLowMemoryMode();

	Mono<Void> close();

	Mono<Void> flush();

	Mono<Void> refresh(boolean force);

	private static <T, U> ValueTransformer<T, U> getValueGetterTransformer(ValueGetter<T, U> valueGetter) {
		return new ValueTransformer<T, U>() {
			@Override
			public <X> Flux<Tuple3<X, T, U>> transform(Flux<Tuple2<X, T>> keys) {
				return keys.flatMapSequential(key -> valueGetter
						.get(key.getT2())
						.map(result -> Tuples.of(key.getT1(), key.getT2(), result)));
			}
		};
	}
}
