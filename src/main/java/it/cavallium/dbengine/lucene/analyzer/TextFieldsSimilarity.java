package it.cavallium.dbengine.lucene.analyzer;

public enum TextFieldsSimilarity {
	BM25Classic,
	NGramBM25Classic,
	BM25L,
	NGramBM25L,
	BM25Plus,
	NGramBM25Plus,
	BM15Plus,
	NGramBM15Plus,
	BM11Plus,
	NGramBM11Plus,
	Classic,
	NGramClassic,
	LTC,
	LDP,
	LDPNoLength,
	Robertson,
	Boolean
}
