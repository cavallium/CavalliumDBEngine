package it.cavallium.dbengine.database.indicizer;

import it.cavallium.dbengine.database.DatabaseMemoryMode;
import it.cavallium.dbengine.database.LLScoreMode;
import it.cavallium.dbengine.database.LLSearchResult;
import it.cavallium.dbengine.database.LLSort;
import it.cavallium.dbengine.database.collections.Joiner;
import it.cavallium.dbengine.database.collections.Joiner.ValueGetter;
import it.cavallium.dbengine.lucene.serializer.Query;
import org.jetbrains.annotations.Nullable;
import it.cavallium.dbengine.client.CompositeSnapshot;
import reactor.core.publisher.Mono;

@SuppressWarnings("SpellCheckingInspection")
public class JoinedIndicizerWriter<KEY, DBTYPE, JOINEDTYPE> implements LuceneIndicizerWriter<KEY, DBTYPE> {

	private final LuceneIndicizerWriter<KEY, JOINEDTYPE> indicizerWriter;
	private final Joiner<KEY, DBTYPE, JOINEDTYPE> joiner;
	private final ValueGetter<KEY, DBTYPE> dbValueGetter;

	public JoinedIndicizerWriter(LuceneIndicizerWriter<KEY, JOINEDTYPE> indicizerWriter,
			Joiner<KEY, DBTYPE, JOINEDTYPE> joiner,
			ValueGetter<KEY, DBTYPE> dbValueGetter) {
		this.indicizerWriter = indicizerWriter;
		this.joiner = joiner;
		this.dbValueGetter = dbValueGetter;
	}

	@Override
	public Mono<Void> add(KEY key, DBTYPE value) {
		return joiner
				.join(dbValueGetter, value)
				.flatMap(joinedValue -> this.indicizerWriter.add(key, joinedValue));
	}

	@Override
	public Mono<Void> remove(KEY key) {
		return this.indicizerWriter.remove(key);
	}

	@Override
	public Mono<Void> update(KEY key, DBTYPE value) {
		return joiner
				.join(dbValueGetter, value)
				.flatMap(joinedValue -> this.indicizerWriter.update(key, joinedValue));
	}

	@Override
	public Mono<Void> clearIndex() {
		return this.indicizerWriter.clearIndex();
	}

	@Override
	public Mono<LLSearchResult> moreLikeThis(@Nullable CompositeSnapshot snapshot, DBTYPE mltDocumentValue, int limit) {
		return joiner
				.join(dbValueGetter, mltDocumentValue)
				.flatMap(val -> this.indicizerWriter.moreLikeThis(snapshot, val, limit));
	}

	@Override
	public Mono<LLSearchResult> search(@Nullable CompositeSnapshot snapshot,
			Query query,
			int limit,
			@Nullable LLSort sort,
			LLScoreMode scoreMode) {
		return this.indicizerWriter
				.search(snapshot, query, limit, sort, scoreMode);
	}

	@Override
	public Mono<Long> count(@Nullable CompositeSnapshot snapshot, Query query) {
		return this.indicizerWriter.count(snapshot, query);
	}

	@Override
	public Mono<Void> close() {
		return this.indicizerWriter.close();
	}

	@Override
	public Mono<KEY> getKey(String key) {
		return this.indicizerWriter.getKey(key);
	}

	@Override
	public DatabaseMemoryMode getMemoryMode() {
		return this.indicizerWriter.getMemoryMode();
	}
}
