package it.cavallium.dbengine.lucene.analyzer;

public enum TextFieldsAnalyzer {
	PartialWords,
	PartialWordsEdge,
	PartialString,
	PartialStringEdge,
	Standard,
	WordSimple,
	WordWithStopwordsStripping,
	WordWithStemming,
	FullText,
}
