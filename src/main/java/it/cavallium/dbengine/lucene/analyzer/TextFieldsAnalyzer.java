package it.cavallium.dbengine.lucene.analyzer;

public enum TextFieldsAnalyzer {
	N4GramPartialWords,
	N4GramPartialWordsEdge,
	N4GramPartialString,
	N4GramPartialStringEdge,
	N3To5GramPartialWords,
	N3To5GramPartialWordsEdge,
	N3To5GramPartialString,
	N3To5GramPartialStringEdge,
	Standard,
	WordSimple,
	ICUCollationKey,
	WordWithStopwordsStripping,
	WordWithStemming,
	FullText,
}
