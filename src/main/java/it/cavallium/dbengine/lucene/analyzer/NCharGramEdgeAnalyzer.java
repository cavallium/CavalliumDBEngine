package it.cavallium.dbengine.lucene.analyzer;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.ngram.EdgeNGramTokenizer;

public class NCharGramEdgeAnalyzer extends Analyzer {

	private final int minGram;
	private final int maxGram;

	public NCharGramEdgeAnalyzer(int minGram, int maxGram) {
		this.minGram = minGram;
		this.maxGram = maxGram;
	}

	@Override
	protected TokenStreamComponents createComponents(final String fieldName) {
		Tokenizer tokenizer = new EdgeNGramTokenizer(minGram, maxGram);
		return new TokenStreamComponents(tokenizer);
	}

}
