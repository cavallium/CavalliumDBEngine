package it.cavallium.dbengine.database;

import io.netty.buffer.ByteBufAllocator;
import it.cavallium.dbengine.client.IndicizerAnalyzers;
import it.cavallium.dbengine.client.IndicizerSimilarities;
import it.cavallium.dbengine.database.disk.DatabaseOptions;
import it.cavallium.dbengine.lucene.analyzer.TextFieldsAnalyzer;
import it.cavallium.dbengine.lucene.analyzer.TextFieldsSimilarity;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import reactor.core.publisher.Mono;

@SuppressWarnings("UnusedReturnValue")
public interface LLDatabaseConnection {

	ByteBufAllocator getAllocator();

	Mono<? extends LLDatabaseConnection> connect();

	Mono<? extends LLKeyValueDatabase> getDatabase(String name,
			List<Column> columns,
			DatabaseOptions databaseOptions);

	Mono<? extends LLLuceneIndex> getLuceneIndex(String name,
			int instancesCount,
			IndicizerAnalyzers indicizerAnalyzers,
			IndicizerSimilarities indicizerSimilarities,
			Duration queryRefreshDebounceTime,
			Duration commitDebounceTime,
			boolean lowMemory,
			boolean inMemory);

	Mono<Void> disconnect();
}
