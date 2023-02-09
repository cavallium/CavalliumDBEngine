package it.cavallium.dbengine.database;

import io.micrometer.core.instrument.MeterRegistry;
import it.cavallium.dbengine.lucene.LuceneHacks;
import it.cavallium.dbengine.rpc.current.data.Column;
import it.cavallium.dbengine.rpc.current.data.DatabaseOptions;
import it.cavallium.dbengine.rpc.current.data.IndicizerAnalyzers;
import it.cavallium.dbengine.rpc.current.data.IndicizerSimilarities;
import it.cavallium.dbengine.rpc.current.data.LuceneIndexStructure;
import it.cavallium.dbengine.rpc.current.data.LuceneOptions;
import java.io.IOException;
import java.util.List;
import org.jetbrains.annotations.Nullable;

@SuppressWarnings("UnusedReturnValue")
public interface LLDatabaseConnection {

	MeterRegistry getMeterRegistry();

	LLDatabaseConnection connect();

	LLKeyValueDatabase getDatabase(String name,
			List<Column> columns,
			DatabaseOptions databaseOptions);

	LLLuceneIndex getLuceneIndex(String clusterName,
			LuceneIndexStructure indexStructure,
			IndicizerAnalyzers indicizerAnalyzers,
			IndicizerSimilarities indicizerSimilarities,
			LuceneOptions luceneOptions,
			@Nullable LuceneHacks luceneHacks);

	void disconnect();
}
