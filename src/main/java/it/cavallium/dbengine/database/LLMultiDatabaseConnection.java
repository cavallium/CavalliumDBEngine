package it.cavallium.dbengine.database;

import static it.cavallium.dbengine.utils.StreamUtils.ROCKSDB_SCHEDULER;
import static it.cavallium.dbengine.utils.StreamUtils.collectOn;
import static it.cavallium.dbengine.utils.StreamUtils.executing;

import com.google.common.collect.Multimap;
import io.micrometer.core.instrument.MeterRegistry;
import it.cavallium.dbengine.client.ConnectionSettings.ConnectionPart;
import it.cavallium.dbengine.client.ConnectionSettings.ConnectionPart.ConnectionPartLucene;
import it.cavallium.dbengine.client.ConnectionSettings.ConnectionPart.ConnectionPartRocksDB;
import it.cavallium.dbengine.lucene.LuceneHacks;
import it.cavallium.dbengine.lucene.LuceneUtils;
import it.cavallium.dbengine.rpc.current.data.Column;
import it.cavallium.dbengine.rpc.current.data.DatabaseOptions;
import it.cavallium.dbengine.rpc.current.data.LuceneIndexStructure;
import it.cavallium.dbengine.rpc.current.data.LuceneOptions;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.CompletionException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;

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
	public MeterRegistry getMeterRegistry() {
		return anyConnection.getMeterRegistry();
	}

	@Override
	public LLDatabaseConnection connect() {
		collectOn(ROCKSDB_SCHEDULER, allConnections.stream(), executing(connection -> {
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
	public LLLuceneIndex getLuceneIndex(String clusterName,
			LuceneIndexStructure indexStructure,
			it.cavallium.dbengine.rpc.current.data.IndicizerAnalyzers indicizerAnalyzers,
			it.cavallium.dbengine.rpc.current.data.IndicizerSimilarities indicizerSimilarities,
			LuceneOptions luceneOptions,
			@Nullable LuceneHacks luceneHacks) {
		IntSet registeredShards = new IntOpenHashSet();
		Map<LLDatabaseConnection, IntSet> connectionToShardMap = new HashMap<>();
		for (int activeShard : indexStructure.activeShards()) {
			if (activeShard >= indexStructure.totalShards()) {
				throw new IllegalArgumentException(
						"ActiveShard " + activeShard + " is bigger than total shards count " + indexStructure.totalShards());
			}
			if (!registeredShards.add(activeShard)) {
				throw new IllegalArgumentException("ActiveShard " + activeShard + " has been specified twice");
			}
			var shardName = LuceneUtils.getStandardName(clusterName, activeShard);
			var connection = luceneShardConnections.getOrDefault(shardName, defaultLuceneConnection);
			Objects.requireNonNull(connection, "Null connection");
			connectionToShardMap.computeIfAbsent(connection, k -> new IntOpenHashSet()).add(activeShard);
		}
		if (connectionToShardMap.keySet().size() == 1) {
			return connectionToShardMap
					.keySet()
					.stream()
					.findFirst()
					.orElseThrow()
					.getLuceneIndex(clusterName,
							indexStructure,
							indicizerAnalyzers,
							indicizerSimilarities,
							luceneOptions,
							luceneHacks
					);
		} else {
			record ShardToIndex(int shard, LLLuceneIndex connIndex) {}
			var luceneIndices = new LLLuceneIndex[indexStructure.totalShards()];
			connectionToShardMap.entrySet().stream().flatMap(entry -> {
				var connectionIndexStructure = indexStructure.setActiveShards(new IntArrayList(entry.getValue()));

				LLLuceneIndex connIndex = entry.getKey().getLuceneIndex(clusterName, connectionIndexStructure,
						indicizerAnalyzers, indicizerSimilarities, luceneOptions, luceneHacks);

				return entry.getValue().intStream().mapToObj(shard -> new ShardToIndex(shard, connIndex));
			}).forEach(index -> luceneIndices[index.shard] = index.connIndex);
			return new LLMultiLuceneIndex(clusterName,
					indexStructure,
					indicizerAnalyzers,
					indicizerSimilarities,
					luceneOptions,
					luceneHacks,
					luceneIndices
			);
		}
	}

	@Override
	public void disconnect() {
		collectOn(ROCKSDB_SCHEDULER, allConnections.stream(), executing(connection -> {
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
				.add("luceneShardConnections=" + luceneShardConnections)
				.add("allConnections=" + allConnections)
				.add("defaultDatabaseConnection=" + defaultDatabaseConnection)
				.add("defaultLuceneConnection=" + defaultLuceneConnection)
				.add("anyConnection=" + anyConnection)
				.toString();
	}
}
