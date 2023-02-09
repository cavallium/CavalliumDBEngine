package it.cavallium.dbengine.client;

import it.cavallium.dbengine.client.query.ClientQueryParams;
import it.cavallium.dbengine.client.query.current.data.Query;
import it.cavallium.dbengine.client.query.current.data.TotalHitsCount;
import it.cavallium.dbengine.database.Delta;
import it.cavallium.dbengine.database.LLSnapshottable;
import it.cavallium.dbengine.lucene.collector.Buckets;
import it.cavallium.dbengine.lucene.searcher.BucketParams;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Stream;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public interface LuceneIndex<T, U> extends LLSnapshottable {

	void addDocument(T key, U value);

	long addDocuments(boolean atomic, Stream<Entry<T, U>> entries);

	void deleteDocument(T key);

	void updateDocument(T key, @NotNull U value);

	long updateDocuments(Stream<Entry<T, U>> entries);

	default void updateOrDeleteDocument(T key, @Nullable U value) {
		if (value == null) {
			deleteDocument(key);
		} else {
			updateDocument(key, value);
		}
	}

	default void updateOrDeleteDocumentIfModified(T key, @NotNull Delta<U> delta) {
		updateOrDeleteDocumentIfModified(key, delta.current(), delta.isModified());
	}

	default void updateOrDeleteDocumentIfModified(T key, @Nullable U currentValue, boolean modified) {
		if (modified) {
			updateOrDeleteDocument(key, currentValue);
		}
	}

	void deleteAll();

	Hits<HitKey<T>> moreLikeThis(ClientQueryParams queryParams, T key,
			U mltDocumentValue);

	Hits<HitKey<T>> search(ClientQueryParams queryParams);

	Buckets computeBuckets(@Nullable CompositeSnapshot snapshot,
			@NotNull List<Query> queries,
			@Nullable Query normalizationQuery,
			BucketParams bucketParams);

	TotalHitsCount count(@Nullable CompositeSnapshot snapshot, Query query);

	boolean isLowMemoryMode();

	void close();

	void flush();

	void waitForMerges();

	void waitForLastMerges();

	void refresh(boolean force);
}
