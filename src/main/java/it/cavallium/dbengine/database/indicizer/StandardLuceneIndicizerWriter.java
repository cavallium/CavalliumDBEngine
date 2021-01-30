package it.cavallium.dbengine.database.indicizer;

import it.cavallium.dbengine.database.LLLuceneIndex;
import it.cavallium.dbengine.database.LLScoreMode;
import it.cavallium.dbengine.database.LLSearchResult;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.LLSort;
import it.cavallium.dbengine.database.LLTerm;
import java.util.Set;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import it.cavallium.dbengine.client.CompositeSnapshot;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

public class StandardLuceneIndicizerWriter<T, U> implements LuceneIndicizerWriter<T, U> {

	protected final LLLuceneIndex luceneIndex;
	protected final Indicizer<T, U> indicizer;

	public StandardLuceneIndicizerWriter(@NotNull LLLuceneIndex luceneIndex, @NotNull Indicizer<T, U> indicizer) {
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
	public Mono<Void> add(@NotNull T key, @NotNull U value) {
		LLTerm docKey = indicizer.toIndex(key);
		return indicizer.toDocument(key, value).flatMap(doc -> luceneIndex.addDocument(docKey, doc));
	}

	@Override
	public Mono<Void> remove(@NotNull T key) {
		LLTerm term = indicizer.toIndex(key);
		return luceneIndex.deleteDocument(term);
	}

	@Override
	public Mono<Void> update(@NotNull T key, @NotNull U value) {
		LLTerm term = indicizer.toIndex(key);
		return indicizer.toDocument(key, value).flatMap(doc -> luceneIndex.updateDocument(term, doc));
	}

	@Override
	public Mono<Void> clearIndex() {
		return luceneIndex.deleteAll();
	}

	@Override
	public Mono<LLSearchResult> moreLikeThis(@Nullable CompositeSnapshot snapshot, U mltDocumentValue, int limit) {
		Flux<Tuple2<String, Set<String>>> mltDocumentFields = indicizer.getMoreLikeThisDocumentFields(mltDocumentValue);
		return luceneIndex.moreLikeThis(resolveSnapshot(snapshot), mltDocumentFields, limit, indicizer.getKeyFieldName());
	}

	@Override
	public Mono<LLSearchResult> search(@Nullable CompositeSnapshot snapshot,
			String query,
			int limit,
			@Nullable LLSort sort,
			LLScoreMode scoreMode) {
		return luceneIndex.search(resolveSnapshot(snapshot), query, limit, sort, scoreMode, indicizer.getKeyFieldName());
	}

	@Override
	public Mono<Long> count(@Nullable CompositeSnapshot snapshot, String query) {
		return luceneIndex.count(resolveSnapshot(snapshot), query);
	}

	@Override
	public Mono<Void> close() {
		return luceneIndex.close();
	}

	@Override
	public Mono<T> getKey(String key) {
		return Mono.just(indicizer.getKey(key));
	}

	@Override
	public DatabaseMemoryMode getMemoryMode() {
		return luceneIndex.isLowMemoryMode() ? DatabaseMemoryMode.LOW : DatabaseMemoryMode.NORMAL;
	}
}
