package it.cavallium.dbengine.lucene.analyzer;

import it.cavallium.dbengine.lucene.LuceneUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.ngram.EdgeNGramTokenFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;

public class NCharGramEdgeAnalyzer extends Analyzer {

	private final boolean words;
	private final int minGram;
	private final int maxGram;

	public NCharGramEdgeAnalyzer(boolean words, int minGram, int maxGram) {
		this.words = words;
		this.minGram = minGram;
		this.maxGram = maxGram;
	}

	@Override
	protected TokenStreamComponents createComponents(final String fieldName) {
		Tokenizer tokenizer;
		TokenStream tokenStream;
		if (words) {
			tokenizer = new StandardTokenizer();
			tokenStream = tokenizer;
		} else {
			tokenizer = new KeywordTokenizer();
			tokenStream = tokenizer;
		}
		tokenStream = LuceneUtils.newCommonFilter(tokenStream, words);
		tokenStream = new EdgeNGramTokenFilter(tokenStream, minGram, maxGram, false);

		return new TokenStreamComponents(tokenizer, tokenStream);
	}

	@Override
	protected TokenStream normalize(String fieldName, TokenStream in) {
		TokenStream tokenStream = in;
		tokenStream = LuceneUtils.newCommonNormalizer(tokenStream);
		return tokenStream;
	}
}
