package it.cavallium.dbengine.client;

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
import java.util.concurrent.locks.LockSupport;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.warp.commonutils.log.Logger;
import org.warp.commonutils.log.LoggerFactory;
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
	private static final Cleaner cleaner = Cleaner.create();
	private final LLLuceneIndex luceneIndex;
	private final Indicizer<T,U> indicizer;
	private final Many<IndexAction> actions;
	private final Empty<Void> actionsClosed;

	public LuceneIndexImpl(LLLuceneIndex luceneIndex, Indicizer<T, U> indicizer) {
		this.luceneIndex = luceneIndex;
		this.indicizer = indicizer;
		this.actions = Sinks
				.many()
				.unicast()
				.onBackpressureBuffer(Queues.<IndexAction>get(1024).get());
		this.actionsClosed = Sinks.empty();

		subscribeToActions();
	}

	private void subscribeToActions() {
		var d = actions
				.asFlux()
				.doAfterTerminate(actionsClosed::tryEmitEmpty)
				.flatMap(this::onParallelAction)
				.concatMap(this::onOrderedAction)
				.then()
				.subscribeOn(Schedulers.boundedElastic())
				.subscribe();

		cleaner.register(LuceneIndexImpl.this, d::dispose);
	}

	/**
	 * Actions that don't require any kind of order
	 */
	private Mono<IndexAction> onParallelAction(IndexAction action) {
		return (switch (action.getType()) {
			case TAKE_SNAPSHOT, RELEASE_SNAPSHOT, FLUSH, CLOSE -> Mono.empty();

			case ADD -> luceneIndex.addDocument(((Add) action).key(), ((Add) action).doc())
					.doOnError(e -> ((Add) action).addedFuture().error(e))
					.onErrorResume(ex -> Mono.empty())
					.doOnSuccess(s -> ((Add) action).addedFuture().success());
			case ADD_MULTI -> luceneIndex.addDocuments(((AddMulti) action).docsFlux())
					.doOnError(e -> ((AddMulti) action).addedMultiFuture().error(e))
					.onErrorResume(ex -> Mono.empty())
					.doOnSuccess(s -> ((AddMulti) action).addedMultiFuture().success());
			case UPDATE -> luceneIndex
					.updateDocument(((Update) action).key(),((Update) action).doc())
					.doOnError(e -> ((Update) action).updatedFuture().error(e))
					.onErrorResume(ex -> Mono.empty())
					.doOnSuccess(s -> ((Update) action).updatedFuture().success());
			case UPDATE_MULTI -> luceneIndex.updateDocuments(Mono.just(((UpdateMulti) action).docs()))
					.doOnError(e -> ((UpdateMulti) action).updatedMultiFuture().error(e))
					.onErrorResume(ex -> Mono.empty())
					.doOnSuccess(s -> ((UpdateMulti) action).updatedMultiFuture().success());
			case DELETE -> luceneIndex.deleteDocument(((Delete) action).key())
					.doOnError(e -> ((Delete) action).deletedFuture().error(e))
					.onErrorResume(ex -> Mono.empty())
					.doOnSuccess(s -> ((Delete) action).deletedFuture().success());
			case DELETE_ALL -> luceneIndex.deleteAll()
					.doOnError(e -> ((DeleteAll) action).deletedAllFuture().error(e))
					.onErrorResume(ex -> Mono.empty())
					.doOnSuccess(s -> ((DeleteAll) action).deletedAllFuture().success());
			case REFRESH -> luceneIndex.refresh(((Refresh) action).force())
					.doOnError(e -> ((Refresh) action).refreshFuture().error(e))
					.onErrorResume(ex -> Mono.empty())
					.doOnSuccess(s -> ((Refresh) action).refreshFuture().success());
		})
				.doOnError(ex -> log.error("Uncaught error when handling parallel index action " + action.getType(), ex))
				.onErrorResume(ex -> Mono.empty())
				.thenReturn(action);
	}

	/**
	 * Actions that require absolute order
	 */
	private Mono<IndexAction> onOrderedAction(IndexAction action) {
		return (switch (action.getType()) {
			case ADD, REFRESH, DELETE_ALL, DELETE, UPDATE_MULTI, UPDATE, ADD_MULTI -> Mono.empty();

			case TAKE_SNAPSHOT -> luceneIndex.takeSnapshot().single()
					.doOnError(e -> ((TakeSnapshot) action).snapshotFuture().error(e))
					.onErrorResume(ex -> Mono.empty())
					.doOnNext(s -> ((TakeSnapshot) action).snapshotFuture().success(s));
			case RELEASE_SNAPSHOT -> luceneIndex.releaseSnapshot(((ReleaseSnapshot) action).snapshot())
					.doOnError(e -> ((ReleaseSnapshot) action).releasedFuture().error(e))
					.onErrorResume(ex -> Mono.empty())
					.doOnSuccess(s -> ((ReleaseSnapshot) action).releasedFuture().success());
			case FLUSH -> luceneIndex.flush()
					.doOnError(e -> ((Flush) action).flushFuture().error(e))
					.onErrorResume(ex -> Mono.empty())
					.doOnSuccess(s -> ((Flush) action).flushFuture().success());
			case CLOSE -> luceneIndex.close()
					.doOnError(e -> ((Close) action).closeFuture().error(e))
					.onErrorResume(ex -> Mono.empty())
					.doOnSuccess(s -> ((Close) action).closeFuture().success())
					.doAfterTerminate(() -> emitActionOptimistically(null));
		})
				.doOnError(ex -> log.error("Uncaught error when handling ordered index action " + action.getType(), ex))
				.onErrorResume(ex -> Mono.empty())
				.onErrorResume(ex -> Mono.empty())
				.thenReturn(action);
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
				.flatMap(doc -> Mono
						.create(sink -> emitActionOptimistically(new IndexAction.Add(indicizer.toIndex(key), doc, sink))));
	}

	@Override
	public Mono<Void> addDocuments(Flux<Entry<T, U>> entries) {
		var convertedEntries = entries.flatMap(entry -> indicizer
				.toDocument(entry.getKey(), entry.getValue())
				.map(doc -> Map.entry(indicizer.toIndex(entry.getKey()), doc))
		);
		return Mono.create(sink -> emitActionOptimistically(new IndexAction.AddMulti(convertedEntries, sink)));
	}

	@Override
	public Mono<Void> deleteDocument(T key) {
		LLTerm id = indicizer.toIndex(key);
		return Mono.create(sink -> emitActionOptimistically(new IndexAction.Delete(id, sink)));
	}

	@Override
	public Mono<Void> updateDocument(T key, @NotNull U value) {
		return indicizer
				.toDocument(key, value)
				.flatMap(doc -> Mono.create(sink -> emitActionOptimistically(new Update(indicizer.toIndex(key), doc, sink))));
	}

	@Override
	public Mono<Void> updateDocuments(Flux<Entry<T, U>> entries) {
		return entries
				.flatMap(entry -> indicizer
						.toDocument(entry.getKey(), entry.getValue())
						.map(doc -> Map.entry(indicizer.toIndex(entry.getKey()), doc)))
				.collectMap(Entry::getKey, Entry::getValue)
				.flatMap(docs -> Mono.create(sink -> emitActionOptimistically(new IndexAction.UpdateMulti(docs, sink))));
	}

	@Override
	public Mono<Void> deleteAll() {
		return Mono.create(sink -> emitActionOptimistically(new IndexAction.DeleteAll(sink)));
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
		return Mono
				.<Void>create(sink -> emitActionOptimistically(new Close(sink)))
				.then(this.actionsClosed.asMono());
	}

	private void emitActionOptimistically(IndexAction action) {
		EmitResult emitResult;
		while ((emitResult = (action == null ? actions.tryEmitComplete() : actions.tryEmitNext(action)))
				== EmitResult.FAIL_NON_SERIALIZED || emitResult == EmitResult.FAIL_OVERFLOW) {
			// 10ms
			LockSupport.parkNanos(10000000);
		}
		emitResult.orThrow();
	}

	/**
	 * Flush writes to disk
	 */
	@Override
	public Mono<Void> flush() {
		return Mono.create(sink -> emitActionOptimistically(new IndexAction.Flush(sink)));
	}

	/**
	 * Refresh index searcher
	 */
	@Override
	public Mono<Void> refresh(boolean force) {
		return Mono.create(sink -> emitActionOptimistically(new IndexAction.Refresh(force, sink)));
	}

	@Override
	public Mono<LLSnapshot> takeSnapshot() {
		return Mono.create(sink -> emitActionOptimistically(new IndexAction.TakeSnapshot(sink)));
	}

	@Override
	public Mono<Void> releaseSnapshot(LLSnapshot snapshot) {
		return Mono.create(sink -> emitActionOptimistically(new IndexAction.ReleaseSnapshot(snapshot, sink)));
	}
}
