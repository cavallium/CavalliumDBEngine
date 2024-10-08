package it.cavallium.dbengine.database.disk;

import io.micrometer.core.instrument.MeterRegistry;
import it.cavallium.dbengine.database.LLDatabaseConnection;
import it.cavallium.dbengine.rpc.current.data.Column;
import it.cavallium.dbengine.rpc.current.data.DatabaseOptions;
import it.cavallium.dbengine.utils.DBException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicBoolean;

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
				getDatabasePath(name),
				columns,
				new LinkedList<>(),
				databaseOptions
		);
	}

	public Path getDatabasePath(String databaseName) {
		return getDatabasePath(basePath, databaseName);
	}

	public static Path getDatabasePath(Path basePath, String databaseName) {
		return basePath.resolve("database_" + databaseName);
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
