package it.cavallium.dbengine.lucene;

public sealed interface LLDocElement permits LLFieldDoc, LLScoreDoc {

	int doc();

	float score();

	int shardIndex();
}
