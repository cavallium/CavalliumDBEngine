package it.cavallium.dbengine.database;

import io.micrometer.core.instrument.MeterRegistry;
import io.net5.buffer.api.BufferAllocator;
import it.cavallium.dbengine.client.DatabaseOptions;
import it.cavallium.dbengine.client.IndicizerAnalyzers;
import it.cavallium.dbengine.client.IndicizerSimilarities;
import it.cavallium.dbengine.client.LuceneOptions;
import it.cavallium.dbengine.lucene.LuceneHacks;
import java.util.List;
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

	Mono<? extends LLLuceneIndex> getLuceneIndex(@Nullable String clusterName,
			@Nullable String shardName,
			int instancesCount,
			IndicizerAnalyzers indicizerAnalyzers,
			IndicizerSimilarities indicizerSimilarities,
			LuceneOptions luceneOptions,
			@Nullable LuceneHacks luceneHacks);

	Mono<Void> disconnect();
}
