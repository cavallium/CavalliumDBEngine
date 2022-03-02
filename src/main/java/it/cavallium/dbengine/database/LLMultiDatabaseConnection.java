package it.cavallium.dbengine.database;

import com.google.common.collect.Multimap;
import io.micrometer.core.instrument.MeterRegistry;
import io.net5.buffer.api.BufferAllocator;
import it.cavallium.dbengine.client.ConnectionSettings.ConnectionPart;
import it.cavallium.dbengine.client.ConnectionSettings.ConnectionPart.ConnectionPartLucene;
import it.cavallium.dbengine.client.ConnectionSettings.ConnectionPart.ConnectionPartRocksDB;
import it.cavallium.dbengine.client.IndicizerAnalyzers;
import it.cavallium.dbengine.client.IndicizerSimilarities;
import it.cavallium.dbengine.client.LuceneOptions;
import it.cavallium.dbengine.lucene.LuceneHacks;
import it.cavallium.dbengine.rpc.current.data.Column;
import it.cavallium.dbengine.rpc.current.data.DatabaseOptions;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class LLMultiDatabaseConnection implements LLDatabaseConnection {

	private static final Logger LOG = LogManager.getLogger(LLMultiDatabaseConnection.class);
	private final Map<String, LLDatabaseConnection> databaseShardConnections = new HashMap<>();
	private final Map<String, LLDatabaseConnection> luceneShardConnections = new HashMap<>();
	private final Set<LLDatabaseConnection> allConnections = new HashSet<>();
	private final LLDatabaseConnection defaultDatabaseConnection;
	private final LLDatabaseConnection defaultLuceneConnection;
	private final LLDatabaseConnection anyConnection;

	public LLMultiDatabaseConnection(Multimap<LLDatabaseConnection, ConnectionPart> subConnections) {
		LLDatabaseConnection defaultDatabaseConnection = null;
		LLDatabaseConnection defaultLuceneConnection = null;
		for (Entry<LLDatabaseConnection, ConnectionPart> entry : subConnections.entries()) {
			var subConnectionSettings = entry.getKey();
			var connectionPart = entry.getValue();
			if (connectionPart instanceof ConnectionPartLucene connectionPartLucene) {
				if (connectionPartLucene.name() == null) {
					defaultLuceneConnection = subConnectionSettings;
				} else {
					luceneShardConnections.put(connectionPartLucene.name(), subConnectionSettings);
				}
			} else if (connectionPart instanceof ConnectionPartRocksDB connectionPartRocksDB) {
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
		this.defaultLuceneConnection = defaultLuceneConnection;
		if (defaultDatabaseConnection != null) {
			anyConnection = defaultDatabaseConnection;
		} else if (defaultLuceneConnection != null) {
			anyConnection = defaultLuceneConnection;
		} else {
			anyConnection = subConnections.keySet().stream().findAny().orElse(null);
		}
		if (defaultDatabaseConnection != null) {
			allConnections.add(defaultDatabaseConnection);
		}
		if (defaultLuceneConnection != null) {
			allConnections.add(defaultLuceneConnection);
		}
		allConnections.addAll(luceneShardConnections.values());
		allConnections.addAll(databaseShardConnections.values());
	}

	@Override
	public BufferAllocator getAllocator() {
		return anyConnection.getAllocator();
	}

	@Override
	public MeterRegistry getMeterRegistry() {
		return anyConnection.getMeterRegistry();
	}

	@Override
	public Mono<? extends LLDatabaseConnection> connect() {
		return Flux
				.fromIterable(allConnections)
				.flatMap((LLDatabaseConnection databaseConnection) -> databaseConnection
						.connect()
						.doOnError(ex -> LOG.error("Failed to open connection", ex))
				)
				.then()
				.thenReturn(this);
	}

	@Override
	public Mono<? extends LLKeyValueDatabase> getDatabase(String name,
			List<Column> columns,
			DatabaseOptions databaseOptions) {
		var conn = databaseShardConnections.getOrDefault(name, defaultDatabaseConnection);
		Objects.requireNonNull(conn, "Null connection");
		return conn.getDatabase(name, columns, databaseOptions);
	}

	@Override
	public Mono<? extends LLLuceneIndex> getLuceneIndex(@Nullable String clusterName,
			@Nullable String shardName,
			int instancesCount,
			IndicizerAnalyzers indicizerAnalyzers,
			IndicizerSimilarities indicizerSimilarities,
			LuceneOptions luceneOptions,
			@Nullable LuceneHacks luceneHacks) {
		String indexShardName = Objects.requireNonNullElse(shardName, clusterName);
		Objects.requireNonNull(indexShardName, "ClusterName and ShardName are both null");
		LLDatabaseConnection conn = luceneShardConnections.getOrDefault(indexShardName, defaultLuceneConnection);
		Objects.requireNonNull(conn, "Null connection");
		return conn.getLuceneIndex(clusterName,
				shardName,
				instancesCount,
				indicizerAnalyzers,
				indicizerSimilarities,
				luceneOptions,
				luceneHacks
		);
	}

	@Override
	public Mono<Void> disconnect() {
		return Flux
				.fromIterable(allConnections)
				.flatMap(databaseConnection -> databaseConnection
						.disconnect()
						.doOnError(ex -> LOG.error("Failed to close connection", ex))
						.onErrorResume(ex -> Mono.empty())
				)
				.then();
	}
}
