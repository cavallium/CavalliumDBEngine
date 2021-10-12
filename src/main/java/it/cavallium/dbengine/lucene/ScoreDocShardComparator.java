package it.cavallium.dbengine.lucene;

import java.util.Comparator;

class ScoreDocShardComparator implements Comparator<LLScoreDoc> {

	public static final Comparator<LLScoreDoc> SCORE_DOC_SHARD_COMPARATOR = new ScoreDocShardComparator();

	@Override
	public int compare(LLScoreDoc hitA, LLScoreDoc hitB) {
		if (hitA.score() == hitB.score()) {
			if (hitA.doc() == hitB.doc()) {
				return Integer.compare(hitA.shardIndex(), hitB.shardIndex());
			} else {
				return Integer.compare(hitB.doc(), hitA.doc());
			}
		} else {
			return Float.compare(hitA.score(), hitB.score());
		}
	}
}
