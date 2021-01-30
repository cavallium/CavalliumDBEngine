package it.cavallium.dbengine.database.disk;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import it.cavallium.dbengine.database.Column;
import it.cavallium.dbengine.database.LLDatabaseConnection;
import it.cavallium.dbengine.database.LLLuceneIndex;
import it.cavallium.dbengine.database.analyzer.TextFieldsAnalyzer;
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
	public Mono<Void> connect() {
		return Mono
				.<Void>fromCallable(() -> {
					if (Files.notExists(basePath)) {
						Files.createDirectories(basePath);
					}
					return null;
				})
				.subscribeOn(Schedulers.boundedElastic());
	}

	@Override
	public LLLocalKeyValueDatabase getDatabase(String name, List<Column> columns, boolean lowMemory) throws IOException {
		return new LLLocalKeyValueDatabase(name, basePath.resolve("database_" + name), columns, new LinkedList<>(),
				crashIfWalError, lowMemory);
	}

	@Override
	public LLLuceneIndex getLuceneIndex(String name,
			int instancesCount,
			TextFieldsAnalyzer textFieldsAnalyzer,
			Duration queryRefreshDebounceTime,
			Duration commitDebounceTime,
			boolean lowMemory) throws IOException {
		if (instancesCount != 1) {
			return new LLLocalMultiLuceneIndex(basePath.resolve("lucene"),
					name,
					instancesCount,
					textFieldsAnalyzer,
					queryRefreshDebounceTime,
					commitDebounceTime,
					lowMemory
			);
		} else {
			return new LLLocalLuceneIndex(basePath.resolve("lucene"),
					name,
					textFieldsAnalyzer,
					queryRefreshDebounceTime,
					commitDebounceTime,
					lowMemory
			);
		}
	}

	@Override
	public Mono<Void> disconnect() {
		return Mono.empty();
	}
}
