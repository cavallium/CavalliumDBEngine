package it.cavallium.dbengine.lucene;

public sealed interface LLDoc permits LLSlotDoc, LLFieldDoc, LLScoreDoc {

	int doc();

	float score();

	int shardIndex();
}
