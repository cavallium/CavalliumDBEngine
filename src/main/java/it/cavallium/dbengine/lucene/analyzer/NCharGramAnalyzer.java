package it.cavallium.dbengine.lucene.analyzer;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.ngram.NGramTokenizer;

public class NCharGramAnalyzer extends Analyzer {

	private final int minGram;
	private final int maxGram;

	public NCharGramAnalyzer(int minGram, int maxGram) {
		this.minGram = minGram;
		this.maxGram = maxGram;
	}

	@Override
	protected TokenStreamComponents createComponents(final String fieldName) {
		Tokenizer tokenizer = new NGramTokenizer(minGram, maxGram);
		return new TokenStreamComponents(tokenizer);
	}
}
