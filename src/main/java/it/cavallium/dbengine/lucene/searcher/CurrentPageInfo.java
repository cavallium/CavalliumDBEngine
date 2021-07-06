package it.cavallium.dbengine.lucene.searcher;

import it.cavallium.dbengine.lucene.LuceneUtils;
import java.util.Comparator;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.ScoreDoc;
import org.jetbrains.annotations.Nullable;

record CurrentPageInfo(@Nullable ScoreDoc last, long remainingLimit, int pageIndex) {

	private static final int MAX_ITEMS_PER_PAGE = 500;

	public static final Comparator<ScoreDoc> TIE_BREAKER = Comparator.comparingInt((d) -> d.shardIndex);
	public static final CurrentPageInfo EMPTY_STATUS = new CurrentPageInfo(null, 0, 0);

	int currentPageLimit() {
		if (pageIndex >= 10) { // safety
			return MAX_ITEMS_PER_PAGE;
		}
		var min = Math.min(MAX_ITEMS_PER_PAGE, LuceneUtils.safeLongToInt(pageIndex * (0b1L << pageIndex)));
		assert min > 0;
		return min;
	}
}
