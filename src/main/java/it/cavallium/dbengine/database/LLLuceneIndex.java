package it.cavallium.dbengine.database;

import static it.cavallium.dbengine.utils.StreamUtils.collectOn;
import static it.cavallium.dbengine.utils.StreamUtils.fastReducing;

import com.google.common.collect.Multimap;
import it.cavallium.dbengine.client.IBackuppable;
import it.cavallium.dbengine.client.query.current.data.NoSort;
import it.cavallium.dbengine.client.query.current.data.Query;
import it.cavallium.dbengine.client.query.current.data.QueryParams;
import it.cavallium.dbengine.client.query.current.data.TotalHitsCount;
import it.cavallium.dbengine.lucene.collector.Buckets;
import it.cavallium.dbengine.lucene.searcher.BucketParams;
import it.cavallium.dbengine.utils.StreamUtils;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public interface LLLuceneIndex extends LLSnapshottable, IBackuppable, SafeCloseable {

	String getLuceneIndexName();

	void addDocument(LLTerm id, LLUpdateDocument doc);

	long addDocuments(boolean atomic, Stream<Entry<LLTerm, LLUpdateDocument>> documents);

	void deleteDocument(LLTerm id);

	void update(LLTerm id, LLIndexRequest request);

	long updateDocuments(Stream<Entry<LLTerm, LLUpdateDocument>> documents);

	void deleteAll();

	// todo: add a filterer parameter?
	/**
	 * @param queryParams the limit is valid for each lucene instance. If you have 15 instances, the number of elements
	 *                    returned can be at most <code>limit * 15</code>.
	 *                    <p>
	 *                    The additional query will be used with the moreLikeThis query: "mltQuery AND additionalQuery"
	 * @return the collection has one or more flux
	 */
	Stream<LLSearchResultShard> moreLikeThis(@Nullable LLSnapshot snapshot,
			QueryParams queryParams,
			@Nullable String keyFieldName,
			Multimap<String, String> mltDocumentFields);

	// todo: add a filterer parameter?
	/**
	 * @param queryParams the limit is valid for each lucene instance. If you have 15 instances, the number of elements
	 *                    returned can be at most <code>limit * 15</code>
	 * @return the collection has one or more flux
	 */
	Stream<LLSearchResultShard> search(@Nullable LLSnapshot snapshot,
			QueryParams queryParams,
			@Nullable String keyFieldName);

	/**
	 * @return buckets with each value collected into one of the buckets
	 */
	Buckets computeBuckets(@Nullable LLSnapshot snapshot,
			@NotNull List<Query> queries,
			@Nullable Query normalizationQuery,
			BucketParams bucketParams);

	default TotalHitsCount count(@Nullable LLSnapshot snapshot, Query query, @Nullable Duration timeout) {
		QueryParams params = QueryParams.of(query,
				0,
				0,
				NoSort.of(),
				false,
				timeout == null ? Long.MAX_VALUE : timeout.toMillis()
		);
		return collectOn(StreamUtils.LUCENE_SCHEDULER,
				this.search(snapshot, params, null).map(LLSearchResultShard::totalHitsCount),
				fastReducing(TotalHitsCount.of(0, true),
						(a, b) -> TotalHitsCount.of(a.value() + b.value(), a.exact() && b.exact())
				)
		);
	}

	boolean isLowMemoryMode();

	/**
	 * Flush writes to disk.
	 * This does not commit, it syncs the data to the disk
	 */
	void flush();

	void waitForMerges();

	/**
	 * Wait for the latest pending merge
	 * This disables future merges until shutdown!
	 */
	void waitForLastMerges();

	/**
	 * Refresh index searcher
	 */
	void refresh(boolean force);
}
