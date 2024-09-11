package it.cavallium.dbengine.database;

import static it.cavallium.dbengine.utils.StreamUtils.collect;
import static it.cavallium.dbengine.utils.StreamUtils.executing;

import com.google.common.collect.Multimap;
import io.micrometer.core.instrument.MeterRegistry;
import it.cavallium.dbengine.client.ConnectionSettings.ConnectionPart;
import it.cavallium.dbengine.client.ConnectionSettings.ConnectionPart.ConnectionPartRocksDB;
import it.cavallium.dbengine.rpc.current.data.Column;
import it.cavallium.dbengine.rpc.current.data.DatabaseOptions;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class LLMultiDatabaseConnection implements LLDatabaseConnection {

	private static final Logger LOG = LogManager.getLogger(LLMultiDatabaseConnection.class);
	private final Map<String, LLDatabaseConnection> databaseShardConnections = new HashMap<>();
	private final Set<LLDatabaseConnection> allConnections = new HashSet<>();
	private final LLDatabaseConnection defaultDatabaseConnection;
	private final LLDatabaseConnection anyConnection;

	public LLMultiDatabaseConnection(Multimap<LLDatabaseConnection, ConnectionPart> subConnections) {
		LLDatabaseConnection defaultDatabaseConnection = null;
		for (Entry<LLDatabaseConnection, ConnectionPart> entry : subConnections.entries()) {
			var subConnectionSettings = entry.getKey();
			var connectionPart = entry.getValue();
			if (connectionPart instanceof ConnectionPartRocksDB connectionPartRocksDB) {
				if (connectionPartRocksDB.name() == null) {
					defaultDatabaseConnection = subConnectionSettings;
				} else {
					databaseShardConnections.put(connectionPartRocksDB.name(), subConnectionSettings);
				}
			} else {
				throw new IllegalArgumentException("Unsupported connection part: " + connectionPart);
			}
		}
		this.defaultDatabaseConnection = defaultDatabaseConnection;
		if (defaultDatabaseConnection != null) {
			anyConnection = defaultDatabaseConnection;
		} else {
			anyConnection = subConnections.keySet().stream().findAny().orElse(null);
		}
		if (defaultDatabaseConnection != null) {
			allConnections.add(defaultDatabaseConnection);
		}
		allConnections.addAll(databaseShardConnections.values());
	}

	@Override
	public MeterRegistry getMeterRegistry() {
		return anyConnection.getMeterRegistry();
	}

	@Override
	public LLDatabaseConnection connect() {
		collect(allConnections.stream(), executing(connection -> {
			try {
				connection.connect();
			} catch (Exception ex) {
				LOG.error("Failed to open connection", ex);
			}
		}));
		return this;
	}

	@Override
	public LLKeyValueDatabase getDatabase(String name,
			List<Column> columns,
			DatabaseOptions databaseOptions) {
		var conn = databaseShardConnections.getOrDefault(name, defaultDatabaseConnection);
		Objects.requireNonNull(conn, "Null connection");
		return conn.getDatabase(name, columns, databaseOptions);
	}

	@Override
	public void disconnect() {
		collect(allConnections.stream(), executing(connection -> {
			try {
				connection.disconnect();
			} catch (Exception ex) {
				LOG.error("Failed to close connection", ex);
			}
		}));
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", LLMultiDatabaseConnection.class.getSimpleName() + "[", "]")
				.add("databaseShardConnections=" + databaseShardConnections)
				.add("allConnections=" + allConnections)
				.add("defaultDatabaseConnection=" + defaultDatabaseConnection)
				.add("anyConnection=" + anyConnection)
				.toString();
	}
}
