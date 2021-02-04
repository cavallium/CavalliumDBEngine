package it.cavallium.dbengine.lucene.analyzer;

import it.cavallium.dbengine.database.EnglishItalianStopFilter;
import it.cavallium.dbengine.lucene.LuceneUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.standard.StandardTokenizer;

public class WordAnalyzer extends Analyzer {

	private final boolean removeStopWords;
	private final boolean stem;

	public WordAnalyzer(boolean removeStopWords, boolean stem) {
		this.removeStopWords = removeStopWords;
		this.stem = stem;
	}

	@Override
	protected TokenStreamComponents createComponents(final String fieldName) {
		Tokenizer tokenizer = new StandardTokenizer();
		TokenStream tokenStream = tokenizer;
		//tokenStream = new LengthFilter(tokenStream, 1, 100);
		if (removeStopWords) {
			tokenStream = new EnglishItalianStopFilter(tokenStream);
		}
		tokenStream = LuceneUtils.newCommonFilter(tokenStream, stem);

		return new TokenStreamComponents(tokenizer, tokenStream);
	}

	@Override
	protected TokenStream normalize(String fieldName, TokenStream in) {
		TokenStream tokenStream = in;
		tokenStream = LuceneUtils.newCommonNormalizer(tokenStream);
		return tokenStream;
	}
}
