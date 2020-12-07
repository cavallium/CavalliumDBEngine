package it.cavallium.dbengine.database;

import it.cavallium.dbengine.database.analyzer.TextFieldsAnalyzer;
import java.io.IOException;
import java.time.Duration;
import java.util.List;

public interface LLDatabaseConnection {

	void connect() throws IOException;

	LLKeyValueDatabase getDatabase(String name, List<Column> columns, boolean lowMemory) throws IOException;

	LLLuceneIndex getLuceneIndex(String name,
			int instancesCount,
			TextFieldsAnalyzer textFieldsAnalyzer,
			Duration queryRefreshDebounceTime,
			Duration commitDebounceTime,
			boolean lowMemory) throws IOException;

	void disconnect() throws IOException;

	void ping() throws IOException;

	double getMediumLatencyMillis() throws IOException;
}
