package it.cavallium.dbengine.database;

import it.cavallium.dbengine.database.analyzer.TextFieldsAnalyzer;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import reactor.core.publisher.Mono;

public interface LLDatabaseConnection {

	Mono<Void> connect();

	LLKeyValueDatabase getDatabase(String name, List<Column> columns, boolean lowMemory) throws IOException;

	LLLuceneIndex getLuceneIndex(String name,
			int instancesCount,
			TextFieldsAnalyzer textFieldsAnalyzer,
			Duration queryRefreshDebounceTime,
			Duration commitDebounceTime,
			boolean lowMemory) throws IOException;

	Mono<Void> disconnect();
}
