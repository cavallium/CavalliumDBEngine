package it.cavallium.dbengine.lucene.analyzer;

import com.ibm.icu.text.Collator;
import com.ibm.icu.util.ULocale;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.en.EnglishMinimalStemFilter;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.apache.lucene.analysis.icu.ICUCollationAttributeFactory;
import org.apache.lucene.analysis.icu.ICUFoldingFilter;
import org.apache.lucene.analysis.icu.segmentation.DefaultICUTokenizerConfig;
import org.apache.lucene.analysis.icu.segmentation.ICUTokenizer;
import org.apache.lucene.analysis.it.ItalianLightStemFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.util.ElisionFilter;

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
			var tokenizer = new ICUTokenizer(new DefaultICUTokenizerConfig(false, false));
			TokenStream tokenStream;
			tokenStream = new ElisionFilter(tokenizer, ItaEngStopWords.ITA_DEFAULT_ARTICLES);
			tokenStream = new LowerCaseFilter(tokenStream);
			tokenStream = new StopFilter(tokenStream, ItaEngStopWords.STOP_WORDS_SET);
			tokenStream = new ItalianLightStemFilter(tokenStream);
			tokenStream = new PorterStemFilter(tokenStream);
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
	protected TokenStream normalize(String fieldName, TokenStream tokenStream) {
		if (icu) {
			tokenStream = new LowerCaseFilter(tokenStream);
			tokenStream = new ElisionFilter(tokenStream, ItaEngStopWords.ITA_DEFAULT_ARTICLES);
			return new ICUFoldingFilter(tokenStream);
		} else {
			return new LowerCaseFilter(tokenStream);
		}
	}
}
