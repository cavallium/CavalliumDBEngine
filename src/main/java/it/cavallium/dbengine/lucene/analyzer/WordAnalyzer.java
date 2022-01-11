package it.cavallium.dbengine.lucene.analyzer;

import com.ibm.icu.text.Collator;
import com.ibm.icu.util.ULocale;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.en.EnglishMinimalStemFilter;
import org.apache.lucene.analysis.en.EnglishMinimalStemFilterFactory;
import org.apache.lucene.analysis.icu.ICUCollationAttributeFactory;
import org.apache.lucene.analysis.icu.ICUCollationKeyAnalyzer;
import org.apache.lucene.analysis.icu.ICUFoldingFilter;
import org.apache.lucene.analysis.icu.ICUFoldingFilterFactory;
import org.apache.lucene.analysis.icu.segmentation.ICUTokenizer;
import org.apache.lucene.analysis.it.ItalianLightStemFilter;
import org.apache.lucene.analysis.it.ItalianLightStemFilterFactory;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.StandardTokenizer;

public class WordAnalyzer extends Analyzer {

	private static final Collator ROOT_COLLATOR = Collator.getInstance(ULocale.ROOT);
	private static final ICUCollationAttributeFactory ROOT_ICU_ATTRIBUTE_FACTORY = new ICUCollationAttributeFactory(ROOT_COLLATOR);

	private final boolean icu;
	private final boolean stem;

	public WordAnalyzer(boolean icu, boolean stem) {
		this.icu = icu;
		this.stem = stem;
		if (icu) {
			if (!stem) {
				throw new IllegalArgumentException("stem must be true if icu is true");
			}
		}
	}

	@Override
	protected TokenStreamComponents createComponents(final String fieldName) {
		if (icu) {
			var tokenizer = new KeywordTokenizer(ROOT_ICU_ATTRIBUTE_FACTORY, KeywordTokenizer.DEFAULT_BUFFER_SIZE);
			TokenStream tokenStream = tokenizer;
			tokenStream = new ICUFoldingFilter(tokenStream);
			return new TokenStreamComponents(tokenizer, tokenStream);
		} else {
			var maxTokenLength = StandardAnalyzer.DEFAULT_MAX_TOKEN_LENGTH;
			var standardTokenizer = new StandardTokenizer(new ICUCollationAttributeFactory(Collator.getInstance(ULocale.ROOT)));
			standardTokenizer.setMaxTokenLength(maxTokenLength);
			TokenStream tokenStream = standardTokenizer;
			tokenStream = new LowerCaseFilter(tokenStream);
			if (stem) {
				tokenStream = new ItalianLightStemFilter(new EnglishMinimalStemFilter(tokenStream));
			}
			return new TokenStreamComponents(r -> {
				standardTokenizer.setMaxTokenLength(maxTokenLength);
				standardTokenizer.setReader(r);
			}, tokenStream);
		}
	}

	@Override
	protected TokenStream normalize(String fieldName, TokenStream in) {
		if (icu) {
			return new ICUFoldingFilter(in);
		} else {
			return new LowerCaseFilter(in);
		}
	}
}
