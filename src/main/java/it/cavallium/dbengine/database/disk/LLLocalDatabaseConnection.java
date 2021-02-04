package it.cavallium.dbengine.database.disk;

import it.cavallium.dbengine.database.Column;
import it.cavallium.dbengine.database.LLDatabaseConnection;
import it.cavallium.dbengine.database.LLLuceneIndex;
import it.cavallium.dbengine.lucene.analyzer.TextFieldsAnalyzer;
import it.cavallium.dbengine.lucene.analyzer.TextFieldsSimilarity;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class LLLocalDatabaseConnection implements LLDatabaseConnection {

	private final Path basePath;
	private final boolean crashIfWalError;

	public LLLocalDatabaseConnection(Path basePath, boolean crashIfWalError) {
		this.basePath = basePath;
		this.crashIfWalError = crashIfWalError;
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
	public Mono<LLLocalKeyValueDatabase> getDatabase(String name, List<Column> columns, boolean lowMemory) {
		return Mono
				.fromCallable(() -> new LLLocalKeyValueDatabase(name,
						basePath.resolve("database_" + name),
						columns,
						new LinkedList<>(),
						crashIfWalError,
						lowMemory
				))
				.subscribeOn(Schedulers.boundedElastic());
	}

	@Override
	public Mono<LLLuceneIndex> getLuceneIndex(String name,
			int instancesCount,
			TextFieldsAnalyzer textFieldsAnalyzer,
			TextFieldsSimilarity textFieldsSimilarity,
			Duration queryRefreshDebounceTime,
			Duration commitDebounceTime,
			boolean lowMemory) {
		return Mono
				.fromCallable(() -> {
					if (instancesCount != 1) {
						return new LLLocalMultiLuceneIndex(basePath.resolve("lucene"),
								name,
								instancesCount,
								textFieldsAnalyzer,
								textFieldsSimilarity,
								queryRefreshDebounceTime,
								commitDebounceTime,
								lowMemory
						);
					} else {
						return new LLLocalLuceneIndex(basePath.resolve("lucene"),
								name,
								textFieldsAnalyzer,
								textFieldsSimilarity,
								queryRefreshDebounceTime,
								commitDebounceTime,
								lowMemory,
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
