package it.cavallium.dbengine.lucene;

import java.util.Comparator;

class LLDocElementScoreComparator implements Comparator<LLDocElement> {

	public static final Comparator<LLDocElement> SCORE_DOC_SCORE_ELEM_COMPARATOR = new LLDocElementScoreComparator();

	@Override
	public int compare(LLDocElement hitA, LLDocElement hitB) {
		return Float.compare(hitA.score(), hitB.score());
	}
}
