package it.cavallium.dbengine.database;

import io.micrometer.core.instrument.MeterRegistry;
import io.netty5.buffer.BufferAllocator;
import it.cavallium.dbengine.lucene.LuceneHacks;
import it.cavallium.dbengine.lucene.LuceneRocksDBManager;
import it.cavallium.dbengine.rpc.current.data.Column;
import it.cavallium.dbengine.rpc.current.data.DatabaseOptions;
import it.cavallium.dbengine.rpc.current.data.IndicizerAnalyzers;
import it.cavallium.dbengine.rpc.current.data.IndicizerSimilarities;
import it.cavallium.dbengine.rpc.current.data.LuceneIndexStructure;
import it.cavallium.dbengine.rpc.current.data.LuceneOptions;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Mono;

@SuppressWarnings("UnusedReturnValue")
public interface LLDatabaseConnection {

	BufferAllocator getAllocator();

	MeterRegistry getMeterRegistry();

	Mono<? extends LLDatabaseConnection> connect();

	Mono<? extends LLKeyValueDatabase> getDatabase(String name,
			List<Column> columns,
			DatabaseOptions databaseOptions);

	Mono<? extends LLLuceneIndex> getLuceneIndex(String clusterName,
			LuceneIndexStructure indexStructure,
			IndicizerAnalyzers indicizerAnalyzers,
			IndicizerSimilarities indicizerSimilarities,
			LuceneOptions luceneOptions,
			@Nullable LuceneHacks luceneHacks);

	Mono<Void> disconnect();
}
