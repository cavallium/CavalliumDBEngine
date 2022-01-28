package it.cavallium.dbengine.database;

import com.google.common.collect.Multimap;
import io.net5.buffer.api.Resource;
import io.net5.buffer.api.Send;
import it.cavallium.data.generator.nativedata.Nullablefloat;
import it.cavallium.dbengine.client.query.current.data.NoSort;
import it.cavallium.dbengine.client.query.current.data.Query;
import it.cavallium.dbengine.client.query.current.data.QueryParams;
import it.cavallium.dbengine.client.query.current.data.TotalHitsCount;
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

public interface LLLuceneIndex extends LLSnapshottable {

	String getLuceneIndexName();

	Mono<Void> addDocument(LLTerm id, LLUpdateDocument doc);

	/**
	 * WARNING! This operation is atomic!
	 * Please don't send infinite or huge documents fluxes, because they will
	 * be kept in ram all at once.
	 */
	Mono<Void> addDocuments(Flux<Entry<LLTerm, LLUpdateDocument>> documents);

	Mono<Void> deleteDocument(LLTerm id);

	Mono<Void> update(LLTerm id, LLIndexRequest request);

	Mono<Void> updateDocuments(Mono<Map<LLTerm, LLUpdateDocument>> documents);

	Mono<Void> deleteAll();

	/**
	 * @param queryParams the limit is valid for each lucene instance. If you have 15 instances, the number of elements
	 *                    returned can be at most <code>limit * 15</code>.
	 *                    <p>
	 *                    The additional query will be used with the moreLikeThis query: "mltQuery AND additionalQuery"
	 * @return the collection has one or more flux
	 */
	Mono<LLSearchResultShard> moreLikeThis(@Nullable LLSnapshot snapshot,
			QueryParams queryParams,
			String keyFieldName,
			Multimap<String, String> mltDocumentFields);

	/**
	 * @param queryParams the limit is valid for each lucene instance. If you have 15 instances, the number of elements
	 *                    returned can be at most <code>limit * 15</code>
	 * @return the collection has one or more flux
	 */
	Mono<LLSearchResultShard> search(@Nullable LLSnapshot snapshot, QueryParams queryParams, String keyFieldName);

	/**
	 * @return buckets with each value collected into one of the buckets
	 */
	Mono<Buckets> computeBuckets(@Nullable LLSnapshot snapshot,
			@NotNull List<Query> queries,
			@Nullable Query normalizationQuery,
			BucketParams bucketParams);

	default Mono<TotalHitsCount> count(@Nullable LLSnapshot snapshot, Query query) {
		QueryParams params = QueryParams.of(query, 0, 0, Nullablefloat.empty(), NoSort.of(), false, Long.MAX_VALUE);
		return Mono.from(this.search(snapshot, params, null)
				.map(llSearchResultShard -> {
					try (llSearchResultShard) {
						return llSearchResultShard.totalHitsCount();
					}
				})
				.defaultIfEmpty(TotalHitsCount.of(0, true))
		);
	}

	boolean isLowMemoryMode();

	Mono<Void> close();

	/**
	 * Flush writes to disk.
	 * This does not commit, it syncs the data to the disk
	 */
	Mono<Void> flush();

	/**
	 * Refresh index searcher
	 */
	Mono<Void> refresh(boolean force);
}
