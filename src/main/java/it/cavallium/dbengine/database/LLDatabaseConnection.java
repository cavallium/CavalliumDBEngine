package it.cavallium.dbengine.database;

import io.micrometer.core.instrument.MeterRegistry;
import it.cavallium.dbengine.rpc.current.data.Column;
import it.cavallium.dbengine.rpc.current.data.DatabaseOptions;
import java.util.List;

@SuppressWarnings("UnusedReturnValue")
public interface LLDatabaseConnection {

	MeterRegistry getMeterRegistry();

	LLDatabaseConnection connect();

	LLKeyValueDatabase getDatabase(String name,
			List<Column> columns,
			DatabaseOptions databaseOptions);

	void disconnect();
}
