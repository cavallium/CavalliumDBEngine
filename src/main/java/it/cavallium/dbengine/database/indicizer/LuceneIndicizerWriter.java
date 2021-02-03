package it.cavallium.dbengine.database.indicizer;

import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.DatabaseMemoryMode;
import it.cavallium.dbengine.database.LLScoreMode;
import it.cavallium.dbengine.database.LLSearchResult;
import it.cavallium.dbengine.database.LLSort;
import it.cavallium.dbengine.lucene.serializer.Query;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

public interface LuceneIndicizerWriter<T, U> {

	Mono<Void> add(T key, U value);

	default Mono<Void> addMulti(@NotNull Flux<Tuple2<T, U>> values) {
		return values.flatMap(tuple -> this.add(tuple.getT1(), tuple.getT2())).then();
	}

	Mono<Void> remove(T key);

	Mono<Void> update(T key, U value);

	default Mono<Void> updateMulti(@NotNull Flux<Tuple2<T, U>> values) {
		return values.flatMap(tuple -> this.update(tuple.getT1(), tuple.getT2())).then();
	}

	Mono<Void> clearIndex();

	Mono<LLSearchResult> moreLikeThis(@Nullable CompositeSnapshot snapshot,
			U mltDocumentValue,
			int limit);

	Mono<LLSearchResult> search(@Nullable CompositeSnapshot snapshot,
			Query query,
			int limit,
			@Nullable LLSort sort,
			LLScoreMode scoreMode);

	Mono<Long> count(@Nullable CompositeSnapshot snapshot, Query query);

	Mono<Void> close();

	Mono<T> getKey(String key);

	DatabaseMemoryMode getMemoryMode();
}
