package it.cavallium.dbengine.database.memory;

import io.micrometer.core.instrument.MeterRegistry;
import it.cavallium.dbengine.database.LLDatabaseConnection;
import it.cavallium.dbengine.database.LLKeyValueDatabase;
import it.cavallium.dbengine.database.LLLuceneIndex;
import it.cavallium.dbengine.database.disk.LLLocalLuceneIndex;
import it.cavallium.dbengine.lucene.LuceneHacks;
import it.cavallium.dbengine.lucene.LuceneUtils;
import it.cavallium.dbengine.rpc.current.data.ByteBuffersDirectory;
import it.cavallium.dbengine.rpc.current.data.Column;
import it.cavallium.dbengine.rpc.current.data.DatabaseOptions;
import it.cavallium.dbengine.rpc.current.data.IndicizerAnalyzers;
import it.cavallium.dbengine.rpc.current.data.IndicizerSimilarities;
import it.cavallium.dbengine.rpc.current.data.LuceneIndexStructure;
import it.cavallium.dbengine.rpc.current.data.LuceneOptions;
import it.cavallium.dbengine.rpc.current.data.LuceneOptionsBuilder;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.jetbrains.annotations.Nullable;

public class LLMemoryDatabaseConnection implements LLDatabaseConnection {

	private final AtomicBoolean connected = new AtomicBoolean();
	private final MeterRegistry meterRegistry;

	public LLMemoryDatabaseConnection(MeterRegistry meterRegistry) {
		this.meterRegistry = meterRegistry;
	}

	@Override
	public MeterRegistry getMeterRegistry() {
		return meterRegistry;
	}

	@Override
	public LLDatabaseConnection connect() {
		if (!connected.compareAndSet(false, true)) {
			throw new IllegalStateException("Already connected");
		}
		return this;
	}

	@Override
	public LLKeyValueDatabase getDatabase(String name,
			List<Column> columns,
			DatabaseOptions databaseOptions) {
		return new LLMemoryKeyValueDatabase(meterRegistry, name, columns);
	}

	@Override
	public LLLuceneIndex getLuceneIndex(String clusterName,
			LuceneIndexStructure indexStructure,
			IndicizerAnalyzers indicizerAnalyzers,
			IndicizerSimilarities indicizerSimilarities,
			LuceneOptions luceneOptions,
			@Nullable LuceneHacks luceneHacks) {
		var memoryLuceneOptions = LuceneOptionsBuilder
				.builder(luceneOptions)
				.directoryOptions(new ByteBuffersDirectory())
				.build();
		return new LLLocalLuceneIndex(meterRegistry,
				clusterName,
				0,
				indicizerAnalyzers,
				indicizerSimilarities,
				memoryLuceneOptions,
				luceneHacks
		);
	}

	@Override
	public void disconnect() {
		connected.compareAndSet(true, false);
	}
}
