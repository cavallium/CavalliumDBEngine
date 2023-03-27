package it.cavallium.dbengine.database.disk;

import io.micrometer.core.instrument.MeterRegistry;
import it.cavallium.dbengine.database.LLDatabaseConnection;
import it.cavallium.dbengine.database.LLLuceneIndex;
import it.cavallium.dbengine.lucene.LuceneHacks;
import it.cavallium.dbengine.rpc.current.data.Column;
import it.cavallium.dbengine.rpc.current.data.DatabaseOptions;
import it.cavallium.dbengine.rpc.current.data.IndicizerAnalyzers;
import it.cavallium.dbengine.rpc.current.data.IndicizerSimilarities;
import it.cavallium.dbengine.rpc.current.data.LuceneIndexStructure;
import it.cavallium.dbengine.rpc.current.data.LuceneOptions;
import it.cavallium.dbengine.utils.DBException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicBoolean;
import org.jetbrains.annotations.Nullable;

public class LLLocalDatabaseConnection implements LLDatabaseConnection {

	private final AtomicBoolean connected = new AtomicBoolean();
	private final MeterRegistry meterRegistry;
	private final Path basePath;
	private final boolean inMemory;

	public LLLocalDatabaseConnection(
			MeterRegistry meterRegistry,
			Path basePath,
			boolean inMemory) {
		this.meterRegistry = meterRegistry;
		this.basePath = basePath;
		this.inMemory = inMemory;
	}

	public MeterRegistry getMeterRegistry() {
		return meterRegistry;
	}

	@Override
	public LLDatabaseConnection connect() {
		if (!connected.compareAndSet(false, true)) {
			throw new IllegalStateException("Already connected");
		}
		if (Files.notExists(basePath)) {
			try {
				Files.createDirectories(basePath);
			} catch (IOException e) {
				throw new DBException(e);
			}
		}
		return this;
	}

	@Override
	public LLLocalKeyValueDatabase getDatabase(String name, List<Column> columns, DatabaseOptions databaseOptions) {
		return new LLLocalKeyValueDatabase(meterRegistry,
				name,
				inMemory,
				basePath.resolve("database_" + name),
				columns,
				new LinkedList<>(),
				databaseOptions
		);
	}

	@Override
	public LLLuceneIndex getLuceneIndex(String clusterName,
			LuceneIndexStructure indexStructure,
			IndicizerAnalyzers indicizerAnalyzers,
			IndicizerSimilarities indicizerSimilarities,
			LuceneOptions luceneOptions,
			@Nullable LuceneHacks luceneHacks) {
		if (clusterName == null) {
			throw new IllegalArgumentException("Cluster name must be set");
		}
		if (indexStructure.activeShards().size() != 1) {
			return new LLLocalMultiLuceneIndex(meterRegistry,
					clusterName,
					indexStructure.activeShards(),
					indexStructure.totalShards(),
					indicizerAnalyzers,
					indicizerSimilarities,
					luceneOptions,
					luceneHacks
			);
		} else {
			return new LLLocalLuceneIndex(meterRegistry,
					clusterName,
					indexStructure.activeShards().getInt(0),
					indicizerAnalyzers,
					indicizerSimilarities,
					luceneOptions,
					luceneHacks
			);
		}
	}

	@Override
	public void disconnect() {
		if (connected.compareAndSet(true, false)) {
		}
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", LLLocalDatabaseConnection.class.getSimpleName() + "[", "]")
				.add("connected=" + connected)
				.add("meterRegistry=" + meterRegistry)
				.add("basePath=" + basePath)
				.add("inMemory=" + inMemory)
				.toString();
	}
}
