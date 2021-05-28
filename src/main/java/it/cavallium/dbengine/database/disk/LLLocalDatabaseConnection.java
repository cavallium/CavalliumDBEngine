package it.cavallium.dbengine.database.disk;

import io.netty.buffer.ByteBufAllocator;
import it.cavallium.dbengine.client.IndicizerAnalyzers;
import it.cavallium.dbengine.client.IndicizerSimilarities;
import it.cavallium.dbengine.database.Column;
import it.cavallium.dbengine.database.LLDatabaseConnection;
import it.cavallium.dbengine.database.LLLuceneIndex;
import it.cavallium.dbengine.lucene.analyzer.TextFieldsAnalyzer;
import it.cavallium.dbengine.lucene.analyzer.TextFieldsSimilarity;
import it.cavallium.dbengine.netty.JMXNettyMonitoringManager;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class LLLocalDatabaseConnection implements LLDatabaseConnection {

	static {
		JMXNettyMonitoringManager.initialize();
	}

	private final ByteBufAllocator allocator;
	private final Path basePath;
	private final boolean crashIfWalError;

	public LLLocalDatabaseConnection(ByteBufAllocator allocator, Path basePath, boolean crashIfWalError) {
		this.allocator = allocator;
		this.basePath = basePath;
		this.crashIfWalError = crashIfWalError;
	}

	@Override
	public ByteBufAllocator getAllocator() {
		return allocator;
	}

	@Override
	public Mono<LLDatabaseConnection> connect() {
		return Mono
				.<LLDatabaseConnection>fromCallable(() -> {
					if (Files.notExists(basePath)) {
						Files.createDirectories(basePath);
					}
					return this;
				})
				.subscribeOn(Schedulers.boundedElastic());
	}

	@Override
	public Mono<LLLocalKeyValueDatabase> getDatabase(String name,
			List<Column> columns,
			boolean lowMemory,
			boolean inMemory) {
		return Mono
				.fromCallable(() -> new LLLocalKeyValueDatabase(
						allocator,
						name,
						basePath.resolve("database_" + name),
						columns,
						new LinkedList<>(),
						crashIfWalError,
						lowMemory,
						inMemory
				))
				.subscribeOn(Schedulers.boundedElastic());
	}

	@Override
	public Mono<LLLuceneIndex> getLuceneIndex(String name,
			int instancesCount,
			IndicizerAnalyzers indicizerAnalyzers,
			IndicizerSimilarities indicizerSimilarities,
			Duration queryRefreshDebounceTime,
			Duration commitDebounceTime,
			boolean lowMemory,
			boolean inMemory) {
		return Mono
				.fromCallable(() -> {
					if (instancesCount != 1) {
						return new LLLocalMultiLuceneIndex(basePath.resolve("lucene"),
								name,
								instancesCount,
								indicizerAnalyzers,
								indicizerSimilarities,
								queryRefreshDebounceTime,
								commitDebounceTime,
								lowMemory,
								inMemory
						);
					} else {
						return new LLLocalLuceneIndex(basePath.resolve("lucene"),
								name,
								indicizerAnalyzers,
								indicizerSimilarities,
								queryRefreshDebounceTime,
								commitDebounceTime,
								lowMemory,
								inMemory,
								null
						);
					}
				})
				.subscribeOn(Schedulers.boundedElastic());
	}

	@Override
	public Mono<Void> disconnect() {
		return Mono.empty();
	}
}
