package it.cavallium.dbengine.client;

import it.cavallium.dbengine.lucene.analyzer.TextFieldsAnalyzer;
import it.cavallium.dbengine.lucene.analyzer.TextFieldsSimilarity;
import java.util.Map;

public record IndicizerSimilarities(TextFieldsSimilarity defaultSimilarity, Map<String, TextFieldsSimilarity> fieldSimilarity) {

	public static IndicizerSimilarities of() {
		return of(TextFieldsSimilarity.BM25Standard);
	}

	public static IndicizerSimilarities of(TextFieldsSimilarity defaultSimilarity) {
		return of(defaultSimilarity, Map.of());
	}

	public static IndicizerSimilarities of(TextFieldsSimilarity defaultSimilarity, Map<String, TextFieldsSimilarity> fieldSimilarity) {
		return new IndicizerSimilarities(defaultSimilarity, fieldSimilarity);
	}
}
