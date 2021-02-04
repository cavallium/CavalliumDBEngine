package it.cavallium.dbengine.lucene.analyzer;

import it.cavallium.dbengine.lucene.LuceneUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.ngram.EdgeNGramTokenFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;

public class N4CharGramEdgeAnalyzer extends Analyzer {

	private final boolean words;

	public N4CharGramEdgeAnalyzer(boolean words) {
		this.words = words;
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
		tokenStream = new EdgeNGramTokenFilter(tokenStream, 3, 5, false);

		return new TokenStreamComponents(tokenizer, tokenStream);
	}

	@Override
	protected TokenStream normalize(String fieldName, TokenStream in) {
		TokenStream tokenStream = in;
		tokenStream = LuceneUtils.newCommonNormalizer(tokenStream);
		return tokenStream;
	}
}
