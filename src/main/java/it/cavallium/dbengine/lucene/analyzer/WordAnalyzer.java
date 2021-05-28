package it.cavallium.dbengine.lucene.analyzer;

import com.ibm.icu.text.Collator;
import com.ibm.icu.util.ULocale;
import it.cavallium.dbengine.database.EnglishItalianStopFilter;
import it.cavallium.dbengine.lucene.LuceneUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.icu.ICUCollationAttributeFactory;
import org.apache.lucene.analysis.icu.ICUCollationKeyAnalyzer;
import org.apache.lucene.analysis.miscellaneous.LengthFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;

public class WordAnalyzer extends Analyzer {

	private final boolean icu;
	private final boolean removeStopWords;
	private final boolean stem;

	public WordAnalyzer(boolean icu, boolean removeStopWords, boolean stem) {
		this.icu = icu;
		this.removeStopWords = removeStopWords;
		this.stem = stem;
	}

	@Override
	protected TokenStreamComponents createComponents(final String fieldName) {
		Tokenizer tokenizer;
		if (icu) {
			tokenizer = new StandardTokenizer(new ICUCollationAttributeFactory(Collator.getInstance(ULocale.ROOT)));
		} else {
			tokenizer = new StandardTokenizer();
		}
		TokenStream tokenStream = tokenizer;
		if (stem) {
			tokenStream = new LengthFilter(tokenStream, 1, 120);
		}
		if (!icu) {
			tokenStream = LuceneUtils.newCommonFilter(tokenStream, stem);
		}
		if (removeStopWords) {
			tokenStream = new EnglishItalianStopFilter(tokenStream);
		}

		return new TokenStreamComponents(tokenizer, tokenStream);
	}

	@Override
	protected TokenStream normalize(String fieldName, TokenStream in) {
		TokenStream tokenStream = in;
		tokenStream = LuceneUtils.newCommonNormalizer(tokenStream);
		return tokenStream;
	}
}
