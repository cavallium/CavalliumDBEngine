package it.cavallium.dbengine.database;

import it.cavallium.dbengine.database.analyzer.TextFieldsAnalyzer;
import java.time.Duration;
import java.util.List;
import reactor.core.publisher.Mono;

@SuppressWarnings("UnusedReturnValue")
public interface LLDatabaseConnection {

	Mono<? extends LLDatabaseConnection> connect();

	Mono<? extends LLKeyValueDatabase> getDatabase(String name, List<Column> columns, boolean lowMemory);

	Mono<? extends LLLuceneIndex> getLuceneIndex(String name,
			int instancesCount,
			TextFieldsAnalyzer textFieldsAnalyzer,
			Duration queryRefreshDebounceTime,
			Duration commitDebounceTime,
			boolean lowMemory);

	Mono<Void> disconnect();
}
