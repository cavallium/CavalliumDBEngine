package it.cavallium.dbengine.client;

import it.cavallium.dbengine.lucene.analyzer.TextFieldsSimilarity;
import java.util.Map;

public class IndicizerSimilarities {

	public static it.cavallium.dbengine.rpc.current.data.IndicizerSimilarities of() {
		return of(TextFieldsSimilarity.BM25Standard);
	}

	public static it.cavallium.dbengine.rpc.current.data.IndicizerSimilarities of(TextFieldsSimilarity defaultSimilarity) {
		return of(defaultSimilarity, Map.of());
	}

	public static it.cavallium.dbengine.rpc.current.data.IndicizerSimilarities of(TextFieldsSimilarity defaultSimilarity,
			Map<String, TextFieldsSimilarity> fieldSimilarity) {
		return it.cavallium.dbengine.rpc.current.data.IndicizerSimilarities.of(defaultSimilarity, fieldSimilarity);
	}
}
