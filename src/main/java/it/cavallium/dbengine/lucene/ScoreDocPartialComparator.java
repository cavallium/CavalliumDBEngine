package it.cavallium.dbengine.lucene;

import java.util.Comparator;
import org.apache.lucene.search.ScoreDoc;

class ScoreDocPartialComparator implements Comparator<ScoreDoc> {

	public static final Comparator<ScoreDoc> SCORE_DOC_PARTIAL_COMPARATOR = new ScoreDocPartialComparator();

	@Override
	public int compare(ScoreDoc hitA, ScoreDoc hitB) {
		if (hitA.score == hitB.score) {
			return Integer.compare(hitB.doc, hitA.doc);
		} else {
			return Float.compare(hitA.score, hitB.score);
		}
	}
}
