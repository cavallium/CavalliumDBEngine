package it.cavallium.dbengine.database.disk;

import io.micrometer.core.instrument.MeterRegistry;
import io.net5.buffer.api.BufferAllocator;
import it.cavallium.dbengine.client.IndicizerAnalyzers;
import it.cavallium.dbengine.client.IndicizerSimilarities;
import it.cavallium.dbengine.client.LuceneOptions;
import it.cavallium.dbengine.database.LLDatabaseConnection;
import it.cavallium.dbengine.database.LLLuceneIndex;
import it.cavallium.dbengine.lucene.LuceneHacks;
import it.cavallium.dbengine.netty.JMXNettyMonitoringManager;
import it.cavallium.dbengine.rpc.current.data.Column;
import it.cavallium.dbengine.rpc.current.data.DatabaseOptions;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class LLLocalDatabaseConnection implements LLDatabaseConnection {

	static {
		JMXNettyMonitoringManager.initialize();
	}

	private final AtomicBoolean connected = new AtomicBoolean();
	private final BufferAllocator allocator;
	private final MeterRegistry meterRegistry;
	private final Path basePath;
	private final boolean inMemory;
	private final AtomicReference<LLTempLMDBEnv> env = new AtomicReference<>();

	public LLLocalDatabaseConnection(BufferAllocator allocator,
			MeterRegistry meterRegistry,
			Path basePath,
			boolean inMemory) {
		this.allocator = allocator;
		this.meterRegistry = meterRegistry;
		this.basePath = basePath;
		this.inMemory = inMemory;
	}

	@Override
	public BufferAllocator getAllocator() {
		return allocator;
	}

	public MeterRegistry getMeterRegistry() {
		return meterRegistry;
	}

	@Override
	public Mono<LLDatabaseConnection> connect() {
		return Mono
				.<LLDatabaseConnection>fromCallable(() -> {
					if (!connected.compareAndSet(false, true)) {
						throw new IllegalStateException("Already connected");
					}
					if (Files.notExists(basePath)) {
						Files.createDirectories(basePath);
					}
					var prev = env.getAndSet(new LLTempLMDBEnv());
					if (prev != null) {
						throw new IllegalStateException("Env was already set");
					}
					return this;
				})
				.subscribeOn(Schedulers.boundedElastic());
	}

	@Override
	public Mono<LLLocalKeyValueDatabase> getDatabase(String name,
			List<Column> columns,
			DatabaseOptions databaseOptions) {
		return Mono
				.fromCallable(() -> new LLLocalKeyValueDatabase(
						allocator,
						meterRegistry,
						name,
						inMemory,
						basePath.resolve("database_" + name),
						columns,
						new LinkedList<>(),
						databaseOptions
				))
				.subscribeOn(Schedulers.boundedElastic());
	}

	@Override
	public Mono<LLLuceneIndex> getLuceneIndex(@Nullable String clusterName,
			@Nullable String shardName,
			int instancesCount,
			IndicizerAnalyzers indicizerAnalyzers,
			IndicizerSimilarities indicizerSimilarities,
			LuceneOptions luceneOptions,
			@Nullable LuceneHacks luceneHacks) {
		return Mono
				.fromCallable(() -> {
					var env = this.env.get();
					if (clusterName == null && shardName == null) {
						throw new IllegalArgumentException("Shard name and/or cluster name must be set");
					}
					if (instancesCount != 1) {
						if (shardName != null && !shardName.equals(clusterName)) {
							throw new IllegalArgumentException("You shouldn't use a shard name for clustered instances");
						}
						Objects.requireNonNull(env, "Environment not set");
						return new LLLocalMultiLuceneIndex(env,
								meterRegistry,
								clusterName,
								instancesCount,
								indicizerAnalyzers,
								indicizerSimilarities,
								luceneOptions,
								luceneHacks
						);
					} else {
						return new LLLocalLuceneIndex(env,
								meterRegistry,
								clusterName,
								shardName,
								indicizerAnalyzers,
								indicizerSimilarities,
								luceneOptions,
								luceneHacks
						);
					}
				})
				.subscribeOn(Schedulers.boundedElastic());
	}

	@Override
	public Mono<Void> disconnect() {
		return Mono.<Void>fromCallable(() -> {
			if (connected.compareAndSet(true, false)) {
				var env = this.env.get();
				if (env != null) {
					env.close();
				}
			}
			return null;
		}).subscribeOn(Schedulers.boundedElastic());
	}
}
