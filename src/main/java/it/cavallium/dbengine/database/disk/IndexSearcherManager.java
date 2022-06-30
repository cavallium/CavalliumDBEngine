package it.cavallium.dbengine.database.disk;

import io.netty5.buffer.api.Send;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.SafeCloseable;
import java.io.IOException;
import java.util.function.Function;
import org.apache.lucene.search.IndexSearcher;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface IndexSearcherManager extends SafeCloseable {

	void maybeRefreshBlocking() throws IOException;

	void maybeRefresh() throws IOException;

	Mono<LLIndexSearcher> retrieveSearcher(@Nullable LLSnapshot snapshot);
}
