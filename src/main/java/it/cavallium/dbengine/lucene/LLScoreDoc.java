package it.cavallium.dbengine.lucene;

import org.apache.lucene.search.ScoreDoc;

public record LLScoreDoc(int doc, float score, int shardIndex) implements LLDoc {

	public ScoreDoc toScoreDoc() {
		return new ScoreDoc(doc, score, shardIndex);
	}
}
