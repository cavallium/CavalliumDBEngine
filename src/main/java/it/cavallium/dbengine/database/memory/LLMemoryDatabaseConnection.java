package it.cavallium.dbengine.database.memory;

import io.micrometer.core.instrument.MeterRegistry;
import io.netty5.buffer.api.BufferAllocator;
import it.cavallium.dbengine.database.LLDatabaseConnection;
import it.cavallium.dbengine.database.LLKeyValueDatabase;
import it.cavallium.dbengine.database.LLLuceneIndex;
import it.cavallium.dbengine.database.disk.LLLocalLuceneIndex;
import it.cavallium.dbengine.database.disk.LLTempLMDBEnv;
import it.cavallium.dbengine.lucene.LuceneHacks;
import it.cavallium.dbengine.lucene.LuceneRocksDBManager;
import it.cavallium.dbengine.netty.JMXNettyMonitoringManager;
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
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class LLMemoryDatabaseConnection implements LLDatabaseConnection {

	static {
		JMXNettyMonitoringManager.initialize();
	}

	private final AtomicBoolean connected = new AtomicBoolean();
	private final BufferAllocator allocator;
	private final MeterRegistry meterRegistry;
	private final AtomicReference<LLTempLMDBEnv> env = new AtomicReference<>();

	public LLMemoryDatabaseConnection(BufferAllocator allocator, MeterRegistry meterRegistry) {
		this.allocator = allocator;
		this.meterRegistry = meterRegistry;
	}

	@Override
	public BufferAllocator getAllocator() {
		return allocator;
	}

	@Override
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
					var prev = env.getAndSet(new LLTempLMDBEnv());
					if (prev != null) {
						throw new IllegalStateException("Env was already set");
					}
					return this;
				})
				.subscribeOn(Schedulers.boundedElastic());
	}

	@Override
	public Mono<LLKeyValueDatabase> getDatabase(String name,
			List<Column> columns,
			DatabaseOptions databaseOptions) {
		return Mono
				.<LLKeyValueDatabase>fromCallable(() -> new LLMemoryKeyValueDatabase(
						allocator,
						meterRegistry,
						name,
						columns
				))
				.subscribeOn(Schedulers.boundedElastic());
	}

	@Override
	public Mono<? extends LLLuceneIndex> getLuceneIndex(String clusterName,
			LuceneIndexStructure indexStructure,
			IndicizerAnalyzers indicizerAnalyzers,
			IndicizerSimilarities indicizerSimilarities,
			LuceneOptions luceneOptions,
			@Nullable LuceneHacks luceneHacks) {
		var memoryLuceneOptions = LuceneOptionsBuilder
				.builder(luceneOptions)
				.directoryOptions(new ByteBuffersDirectory())
				.build();
		return Mono
				.<LLLuceneIndex>fromCallable(() -> {
					var env = this.env.get();
					return new LLLocalLuceneIndex(env,
							meterRegistry,
							clusterName,
							0,
							indicizerAnalyzers,
							indicizerSimilarities,
							memoryLuceneOptions,
							luceneHacks,
							null
					);
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
