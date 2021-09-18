package it.cavallium.dbengine.database.disk;

import io.net5.buffer.api.Send;
import it.cavallium.dbengine.database.LLSnapshot;
import java.io.IOException;
import java.util.function.Function;
import org.apache.lucene.search.IndexSearcher;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface IndexSearcherManager {

	void maybeRefreshBlocking() throws IOException;

	void maybeRefresh() throws IOException;

	<T> Flux<T> searchMany(@Nullable LLSnapshot snapshot, Function<IndexSearcher, Flux<T>> searcherFunction);

	<T> Mono<T> search(@Nullable LLSnapshot snapshot, Function<IndexSearcher, Mono<T>> searcherFunction);

	Mono<Send<LLIndexSearcher>> retrieveSearcher(@Nullable LLSnapshot snapshot);

	Mono<Void> close();
}
