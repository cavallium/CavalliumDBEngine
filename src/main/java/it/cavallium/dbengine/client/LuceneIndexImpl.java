package it.cavallium.dbengine.client;

import com.google.common.util.concurrent.Uninterruptibles;
import io.net5.buffer.api.Send;
import it.cavallium.dbengine.client.IndexAction.Add;
import it.cavallium.dbengine.client.IndexAction.AddMulti;
import it.cavallium.dbengine.client.IndexAction.Close;
import it.cavallium.dbengine.client.IndexAction.Delete;
import it.cavallium.dbengine.client.IndexAction.DeleteAll;
import it.cavallium.dbengine.client.IndexAction.Flush;
import it.cavallium.dbengine.client.IndexAction.Refresh;
import it.cavallium.dbengine.client.IndexAction.ReleaseSnapshot;
import it.cavallium.dbengine.client.IndexAction.TakeSnapshot;
import it.cavallium.dbengine.client.IndexAction.Update;
import it.cavallium.dbengine.client.IndexAction.UpdateMulti;
import it.cavallium.dbengine.client.query.ClientQueryParams;
import it.cavallium.dbengine.client.query.current.data.Query;
import it.cavallium.dbengine.client.query.current.data.TotalHitsCount;
import it.cavallium.dbengine.database.LLLuceneIndex;
import it.cavallium.dbengine.database.LLSearchResultShard;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.LLTerm;
import it.cavallium.dbengine.database.collections.ValueTransformer;
import java.lang.ref.Cleaner;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.LockSupport;
import java.util.logging.Level;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.warp.commonutils.log.Logger;
import org.warp.commonutils.log.LoggerFactory;
import org.warp.commonutils.type.ShortNamedThreadFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.EmitResult;
import reactor.core.publisher.Sinks.Empty;
import reactor.core.publisher.Sinks.Many;
import reactor.core.scheduler.Schedulers;
import reactor.util.concurrent.Queues;
import reactor.util.function.Tuple2;

public class LuceneIndexImpl<T, U> implements LuceneIndex<T, U> {

	private static final Logger log = LoggerFactory.getLogger(LuceneIndex.class);
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
	public Mono<Void> addDocuments(Flux<Entry<T, U>> entries) {
		return luceneIndex
				.addDocuments(entries
						.flatMap(entry -> indicizer
								.toDocument(entry.getKey(), entry.getValue())
								.map(doc -> Map.entry(indicizer.toIndex(entry.getKey()), doc)))
				);
	}

	@Override
	public Mono<Void> deleteDocument(T key) {
		LLTerm id = indicizer.toIndex(key);
		return luceneIndex.deleteDocument(id);
	}

	@Override
	public Mono<Void> updateDocument(T key, @NotNull U value) {
		return indicizer
				.toDocument(key, value)
				.flatMap(doc -> luceneIndex.updateDocument(indicizer.toIndex(key), doc));
	}

	@Override
	public Mono<Void> updateDocuments(Flux<Entry<T, U>> entries) {
		return luceneIndex
				.updateDocuments(entries
						.flatMap(entry -> indicizer
								.toDocument(entry.getKey(), entry.getValue())
								.map(doc -> Map.entry(indicizer.toIndex(entry.getKey()), doc)))
						.collectMap(Entry::getKey, Entry::getValue)
				);
	}

	@Override
	public Mono<Void> deleteAll() {
		return luceneIndex.deleteAll();
	}

	@Override
	public Mono<Send<Hits<HitKey<T>>>> moreLikeThis(ClientQueryParams queryParams,
			T key,
			U mltDocumentValue) {
		Flux<Tuple2<String, Set<String>>> mltDocumentFields
				= indicizer.getMoreLikeThisDocumentFields(key, mltDocumentValue);

		return luceneIndex
				.moreLikeThis(resolveSnapshot(queryParams.snapshot()),
						queryParams.toQueryParams(),
						indicizer.getKeyFieldName(),
						mltDocumentFields
				)
				.map(this::mapResults)
				.single();
	}

	@Override
	public Mono<Send<Hits<HitKey<T>>>> search(ClientQueryParams queryParams) {
		return luceneIndex
				.search(resolveSnapshot(queryParams.snapshot()),
						queryParams.toQueryParams(),
						indicizer.getKeyFieldName()
				)
				.map(this::mapResults)
				.single();
	}

	private Send<Hits<HitKey<T>>> mapResults(Send<LLSearchResultShard> llSearchResultToReceive) {
		var llSearchResult = llSearchResultToReceive.receive();
		var scoresWithKeysFlux = llSearchResult
				.results()
				.map(hit -> new HitKey<>(indicizer.getKey(hit.key()), hit.score()));

		return new Hits<>(scoresWithKeysFlux, llSearchResult.totalHitsCount(), llSearchResult::close).send();
	}

	@Override
	public Mono<TotalHitsCount> count(@Nullable CompositeSnapshot snapshot, Query query) {
		return this
				.search(ClientQueryParams.<LazyHitKey<T>>builder().snapshot(snapshot).query(query).limit(0).build())
				.single()
				.map(searchResultKeysSend -> {
					try (var searchResultKeys = searchResultKeysSend.receive()) {
						return searchResultKeys.totalHitsCount();
					}
				});
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
}
