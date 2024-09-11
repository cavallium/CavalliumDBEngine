package it.cavallium.dbengine.database.memory;

import io.micrometer.core.instrument.MeterRegistry;
import it.cavallium.dbengine.database.LLDatabaseConnection;
import it.cavallium.dbengine.database.LLKeyValueDatabase;
import it.cavallium.dbengine.rpc.current.data.Column;
import it.cavallium.dbengine.rpc.current.data.DatabaseOptions;
import java.util.List;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicBoolean;

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
	public void disconnect() {
		connected.compareAndSet(true, false);
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", LLMemoryDatabaseConnection.class.getSimpleName() + "[", "]")
				.add("connected=" + connected)
				.add("meterRegistry=" + meterRegistry)
				.toString();
	}
}
