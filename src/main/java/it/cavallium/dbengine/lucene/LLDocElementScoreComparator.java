package it.cavallium.dbengine.lucene;

import java.util.Comparator;

class LLDocElementScoreComparator implements Comparator<LLDoc> {

	public static final Comparator<LLDoc> SCORE_DOC_SCORE_ELEM_COMPARATOR = new LLDocElementScoreComparator();

	@Override
	public int compare(LLDoc hitA, LLDoc hitB) {
		return Float.compare(hitB.score(), hitA.score());
	}
}
