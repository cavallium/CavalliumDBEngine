package it.cavallium.dbengine.client;

import it.cavallium.dbengine.lucene.analyzer.TextFieldsAnalyzer;
import java.util.Map;

public record IndicizerAnalyzers(TextFieldsAnalyzer defaultAnalyzer, Map<String, TextFieldsAnalyzer> fieldAnalyzer) {

	public static IndicizerAnalyzers of() {
		return of(TextFieldsAnalyzer.ICUCollationKey);
	}

	public static IndicizerAnalyzers of(TextFieldsAnalyzer defaultAnalyzer) {
		return of(defaultAnalyzer, Map.of());
	}

	public static IndicizerAnalyzers of(TextFieldsAnalyzer defaultAnalyzer, Map<String, TextFieldsAnalyzer> fieldAnalyzer) {
		return new IndicizerAnalyzers(defaultAnalyzer, fieldAnalyzer);
	}
}
