package it.cavallium.dbengine.database;

import com.google.common.collect.Multimap;
import it.cavallium.dbengine.client.IBackuppable;
import it.cavallium.dbengine.client.query.current.data.NoSort;
import it.cavallium.dbengine.client.query.current.data.Query;
import it.cavallium.dbengine.client.query.current.data.QueryParams;
import it.cavallium.dbengine.client.query.current.data.TotalHitsCount;
import it.cavallium.dbengine.lucene.collector.Buckets;
import it.cavallium.dbengine.lucene.searcher.BucketParams;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface LLLuceneIndex extends LLSnapshottable, IBackuppable, SafeCloseable {

	String getLuceneIndexName();

	Mono<Void> addDocument(LLTerm id, LLUpdateDocument doc);

	Mono<Long> addDocuments(boolean atomic, Flux<Entry<LLTerm, LLUpdateDocument>> documents);

	Mono<Void> deleteDocument(LLTerm id);

	Mono<Void> update(LLTerm id, LLIndexRequest request);

	Mono<Long> updateDocuments(Flux<Entry<LLTerm, LLUpdateDocument>> documents);

	Mono<Void> deleteAll();

	/**
	 * @param queryParams the limit is valid for each lucene instance. If you have 15 instances, the number of elements
	 *                    returned can be at most <code>limit * 15</code>.
	 *                    <p>
	 *                    The additional query will be used with the moreLikeThis query: "mltQuery AND additionalQuery"
	 * @return the collection has one or more flux
	 */
	Flux<LLSearchResultShard> moreLikeThis(@Nullable LLSnapshot snapshot,
			QueryParams queryParams,
			@Nullable String keyFieldName,
			Multimap<String, String> mltDocumentFields);

	/**
	 * @param queryParams the limit is valid for each lucene instance. If you have 15 instances, the number of elements
	 *                    returned can be at most <code>limit * 15</code>
	 * @return the collection has one or more flux
	 */
	Flux<LLSearchResultShard> search(@Nullable LLSnapshot snapshot,
			QueryParams queryParams,
			@Nullable String keyFieldName);

	/**
	 * @return buckets with each value collected into one of the buckets
	 */
	Mono<Buckets> computeBuckets(@Nullable LLSnapshot snapshot,
			@NotNull List<Query> queries,
			@Nullable Query normalizationQuery,
			BucketParams bucketParams);

	default Mono<TotalHitsCount> count(@Nullable LLSnapshot snapshot, Query query, @Nullable Duration timeout) {
		QueryParams params = QueryParams.of(query,
				0,
				0,
				NoSort.of(),
				false,
				timeout == null ? Long.MAX_VALUE : timeout.toMillis()
		);
		return Mono
				.usingWhen(this.search(snapshot, params, null).singleOrEmpty(),
						llSearchResultShard -> Mono.just(llSearchResultShard.totalHitsCount()),
						LLUtils::finalizeResource
				)
				.defaultIfEmpty(TotalHitsCount.of(0, true));
	}

	boolean isLowMemoryMode();

	/**
	 * Flush writes to disk.
	 * This does not commit, it syncs the data to the disk
	 */
	Mono<Void> flush();

	Mono<Void> waitForMerges();

	/**
	 * Wait for the latest pending merge
	 * This disables future merges until shutdown!
	 */
	Mono<Void> waitForLastMerges();

	/**
	 * Refresh index searcher
	 */
	Mono<Void> refresh(boolean force);
}
