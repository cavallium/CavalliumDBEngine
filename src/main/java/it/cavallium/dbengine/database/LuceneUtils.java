package it.cavallium.dbengine.database;

import it.cavallium.dbengine.database.analyzer.N4CharGramAnalyzer;
import it.cavallium.dbengine.database.analyzer.N4CharGramEdgeAnalyzer;
import it.cavallium.dbengine.database.analyzer.TextFieldsAnalyzer;
import it.cavallium.dbengine.database.analyzer.WordAnalyzer;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.en.EnglishPossessiveFilter;
import org.apache.lucene.analysis.en.KStemFilter;
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilter;

public class LuceneUtils {
	private static final Analyzer lucene4CharGramAnalyzerEdgeInstance = new N4CharGramEdgeAnalyzer();
	private static final Analyzer lucene4CharGramAnalyzerInstance = new N4CharGramAnalyzer();
	private static final Analyzer luceneWordAnalyzerStopWordsAndStemInstance = new WordAnalyzer(true, true);
	private static final Analyzer luceneWordAnalyzerStopWordsInstance = new WordAnalyzer(true, false);
	private static final Analyzer luceneWordAnalyzerStemInstance = new WordAnalyzer(false, true);
	private static final Analyzer luceneWordAnalyzerSimpleInstance = new WordAnalyzer(false, false);

	public static Analyzer getAnalyzer(TextFieldsAnalyzer analyzer) {
		switch (analyzer) {
			case PartialWordsEdge:
				return lucene4CharGramAnalyzerEdgeInstance;
			case PartialWords:
				return lucene4CharGramAnalyzerInstance;
			case FullText:
				return luceneWordAnalyzerStopWordsAndStemInstance;
			case WordWithStopwordsStripping:
				return luceneWordAnalyzerStopWordsInstance;
			case WordWithStemming:
				return luceneWordAnalyzerStemInstance;
			case WordSimple:
				return luceneWordAnalyzerSimpleInstance;
			default:
				throw new UnsupportedOperationException("Unknown analyzer: " + analyzer);
		}
	}

	/**
	 *
	 * @param stem Enable stem filters on words.
	 *              Pass false if it will be used with a n-gram filter
	 */
	public static TokenStream newCommonFilter(TokenStream tokenStream, boolean stem) {
		tokenStream = newCommonNormalizer(tokenStream);
		if (stem) {
			tokenStream = new KStemFilter(tokenStream);
			tokenStream = new EnglishPossessiveFilter(tokenStream);
		}
		return tokenStream;
	}

	public static TokenStream newCommonNormalizer(TokenStream tokenStream) {
		tokenStream = new ASCIIFoldingFilter(tokenStream);
		tokenStream = new LowerCaseFilter(tokenStream);
		return tokenStream;
	}
}
