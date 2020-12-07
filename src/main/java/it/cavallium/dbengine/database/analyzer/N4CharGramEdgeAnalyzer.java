package it.cavallium.dbengine.database.analyzer;

import it.cavallium.dbengine.database.LuceneUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.ngram.EdgeNGramTokenFilter;

public class N4CharGramEdgeAnalyzer extends Analyzer {

	public N4CharGramEdgeAnalyzer() {

	}

	@Override
	protected TokenStreamComponents createComponents(final String fieldName) {
		Tokenizer tokenizer = new KeywordTokenizer();
		TokenStream tokenStream = tokenizer;
		tokenStream = LuceneUtils.newCommonFilter(tokenStream, false);
		tokenStream = new EdgeNGramTokenFilter(tokenStream, 4, 4, false);

		return new TokenStreamComponents(tokenizer, tokenStream);
	}

	@Override
	protected TokenStream normalize(String fieldName, TokenStream in) {
		TokenStream tokenStream = in;
		tokenStream = LuceneUtils.newCommonNormalizer(tokenStream);
		return tokenStream;
	}
}
