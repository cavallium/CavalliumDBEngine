package it.cavallium.dbengine.client;

import it.cavallium.dbengine.lucene.analyzer.TextFieldsAnalyzer;
import it.cavallium.dbengine.rpc.current.serializers.IndicizerAnalyzersSerializer;
import java.util.Map;

public class IndicizerAnalyzers {

	public static it.cavallium.dbengine.rpc.current.data.IndicizerAnalyzers of() {
		return of(TextFieldsAnalyzer.ICUCollationKey);
	}

	public static it.cavallium.dbengine.rpc.current.data.IndicizerAnalyzers of(TextFieldsAnalyzer defaultAnalyzer) {
		return of(defaultAnalyzer, Map.of());
	}

	public static it.cavallium.dbengine.rpc.current.data.IndicizerAnalyzers of(TextFieldsAnalyzer defaultAnalyzer, Map<String, TextFieldsAnalyzer> fieldAnalyzer) {
		return new it.cavallium.dbengine.rpc.current.data.IndicizerAnalyzers(defaultAnalyzer, fieldAnalyzer);
	}
}
