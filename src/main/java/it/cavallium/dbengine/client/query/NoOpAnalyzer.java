package it.cavallium.dbengine.client.query;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordTokenizer;

public class NoOpAnalyzer extends Analyzer {

	public static final Analyzer INSTANCE = new NoOpAnalyzer();

	public NoOpAnalyzer() {
	}

	@Override
	protected TokenStreamComponents createComponents(String fieldName) {
		return new TokenStreamComponents(new KeywordTokenizer());
	}
}
