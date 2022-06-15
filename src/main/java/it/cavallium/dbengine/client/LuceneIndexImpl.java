package it.cavallium.dbengine.client;

import io.netty5.buffer.api.Resource;
import io.netty5.buffer.api.internal.ResourceSupport;
import it.cavallium.dbengine.client.query.ClientQueryParams;
import it.cavallium.dbengine.client.query.current.data.Query;
import it.cavallium.dbengine.client.query.current.data.TotalHitsCount;
import it.cavallium.dbengine.database.LLKeyScore;
import it.cavallium.dbengine.database.LLLuceneIndex;
import it.cavallium.dbengine.database.LLSearchResult;
import it.cavallium.dbengine.database.LLSearchResultShard;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.LLTerm;
import it.cavallium.dbengine.database.LLUpdateDocument;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.lucene.LuceneUtils;
import it.cavallium.dbengine.lucene.collector.Buckets;
import it.cavallium.dbengine.lucene.searcher.BucketParams;
import it.cavallium.dbengine.utils.SimpleResource;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.logging.Level;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;

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
	public Mono<Long> addDocuments(boolean atomic, Flux<Entry<T, U>> entries) {
		return luceneIndex.addDocuments(atomic, entries.flatMap(entry -> indicizer
				.toDocument(entry.getKey(), entry.getValue())
				.map(doc -> Map.entry(indicizer.toIndex(entry.getKey()), doc))));
	}

	@Override
	public Mono<Void> deleteDocument(T key) {
		LLTerm id = indicizer.toIndex(key);
		return luceneIndex.deleteDocument(id);
	}

	@Override
	public Mono<Void> updateDocument(T key, @NotNull U value) {
		return indicizer
				.toIndexRequest(key, value)
				.flatMap(doc -> luceneIndex.update(indicizer.toIndex(key), doc));
	}

	@Override
	public Mono<Long> updateDocuments(Flux<Entry<T, U>> entries) {
		Flux<Entry<LLTerm, LLUpdateDocument>> mappedEntries = entries
				.flatMap(entry -> Mono
						.zip(Mono.just(indicizer.toIndex(entry.getKey())),
								indicizer.toDocument(entry.getKey(), entry.getValue()).single(),
								Map::entry
						)
						.single()
				)
				.log("impl-update-documents", Level.FINEST, false, SignalType.ON_NEXT, SignalType.ON_COMPLETE);
		return luceneIndex.updateDocuments(mappedEntries);
	}

	@Override
	public Mono<Void> deleteAll() {
		return luceneIndex.deleteAll();
	}

	@Override
	public Mono<Hits<HitKey<T>>> moreLikeThis(ClientQueryParams queryParams,
			T key,
			U mltDocumentValue) {
		var mltDocumentFields
				= indicizer.getMoreLikeThisDocumentFields(key, mltDocumentValue);

		return luceneIndex
				.moreLikeThis(resolveSnapshot(queryParams.snapshot()),
						queryParams.toQueryParams(),
						indicizer.getKeyFieldName(),
						mltDocumentFields
				)
				.collectList()
				.flatMap(shards -> mergeResults(queryParams, shards))
				.map(this::mapResults)
				.single();
	}

	@Override
	public Mono<Hits<HitKey<T>>> search(ClientQueryParams queryParams) {
		return luceneIndex
				.search(resolveSnapshot(queryParams.snapshot()),
						queryParams.toQueryParams(),
						indicizer.getKeyFieldName()
				)
				.collectList()
				.flatMap(shards -> mergeResults(queryParams, shards))
				.map(this::mapResults)
				.single()
				.doOnDiscard(LLSearchResultShard.class, SimpleResource::close)
				.doOnDiscard(Hits.class, SimpleResource::close);
	}

	@Override
	public Mono<Buckets> computeBuckets(@Nullable CompositeSnapshot snapshot,
			@NotNull List<Query> query,
			@Nullable Query normalizationQuery,
			BucketParams bucketParams) {
		return luceneIndex.computeBuckets(resolveSnapshot(snapshot), query,
				normalizationQuery, bucketParams).single();
	}

	private Hits<HitKey<T>> mapResults(LLSearchResultShard llSearchResult) {
		var scoresWithKeysFlux = llSearchResult.results()
				.map(hit -> new HitKey<>(indicizer.getKey(hit.key()), hit.score()));

		return new Hits<>(scoresWithKeysFlux, llSearchResult.totalHitsCount(), llSearchResult::close);
	}

	@Override
	public Mono<TotalHitsCount> count(@Nullable CompositeSnapshot snapshot, Query query) {
		return Mono.usingWhen(this.search(ClientQueryParams
				.builder()
				.snapshot(snapshot)
				.query(query)
				.timeout(Duration.ofSeconds(30))
				.limit(0)
				.build()), searchResultKeys -> {
			return Mono.just(searchResultKeys.totalHitsCount());
		}, LLUtils::finalizeResource);
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
	public Mono<Void> refresh(boolean force) {
		return luceneIndex.refresh(force);
	}

	@Override
	public Mono<LLSnapshot> takeSnapshot() {
		return luceneIndex.takeSnapshot();
	}

	@Override
	public Mono<Void> releaseSnapshot(LLSnapshot snapshot) {
		return luceneIndex.releaseSnapshot(snapshot);
	}

	private static Mono<LLSearchResultShard> mergeResults(ClientQueryParams queryParams,
			List<LLSearchResultShard> shards) {
		if (shards.size() == 0) {
			return Mono.empty();
		} else if (shards.size() == 1) {
			return Mono.just(shards.get(0));
		}
		return Mono.fromCallable(() -> {
			TotalHitsCount count = null;
			ObjectArrayList<Flux<LLKeyScore>> results = new ObjectArrayList<>(shards.size());
			ObjectArrayList<SimpleResource> resources = new ObjectArrayList<>(shards.size());
			for (LLSearchResultShard shard : shards) {
				if (count == null) {
					count = shard.totalHitsCount();
				} else {
					count = LuceneUtils.sum(count, shard.totalHitsCount());
				}
				var maxLimit = queryParams.offset() + queryParams.limit();
				results.add(shard.results().take(maxLimit, true));
				resources.add(shard);
			}
			Objects.requireNonNull(count);
			Flux<LLKeyScore> resultsFlux;
			if (results.size() == 0) {
				resultsFlux = Flux.empty();
			} else if (results.size() == 1) {
				resultsFlux = results.get(0);
			} else {
				resultsFlux = Flux.merge(results);
			}
			return new LLSearchResultShard(resultsFlux, count, () -> {
				for (var resource : resources) {
					resource.close();
				}
			});
		});
	}
}
