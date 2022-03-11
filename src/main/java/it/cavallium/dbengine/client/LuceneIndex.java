package it.cavallium.dbengine.client;

import io.net5.buffer.api.Send;
import it.cavallium.dbengine.client.query.ClientQueryParams;
import it.cavallium.dbengine.client.query.current.data.Query;
import it.cavallium.dbengine.client.query.current.data.TotalHitsCount;
import it.cavallium.dbengine.database.Delta;
import it.cavallium.dbengine.database.LLSnapshottable;
import it.cavallium.dbengine.database.collections.ValueGetter;
import it.cavallium.dbengine.database.collections.ValueTransformer;
import it.cavallium.dbengine.lucene.collector.Buckets;
import it.cavallium.dbengine.lucene.searcher.BucketParams;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import java.util.List;
import java.util.Map.Entry;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface LuceneIndex<T, U> extends LLSnapshottable {

	Mono<Void> addDocument(T key, U value);

	Mono<Void> addDocuments(boolean atomic, Flux<Entry<T, U>> entries);

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

	Mono<Hits<HitKey<T>>> moreLikeThis(ClientQueryParams queryParams, T key,
			U mltDocumentValue);

	Mono<Hits<HitKey<T>>> search(ClientQueryParams queryParams);

	Mono<Buckets> computeBuckets(@Nullable CompositeSnapshot snapshot,
			@NotNull List<Query> queries,
			@Nullable Query normalizationQuery,
			BucketParams bucketParams);

	Mono<TotalHitsCount> count(@Nullable CompositeSnapshot snapshot, Query query);

	boolean isLowMemoryMode();

	Mono<Void> close();

	Mono<Void> flush();

	Mono<Void> refresh(boolean force);
}
