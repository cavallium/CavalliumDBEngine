package it.cavallium.dbengine.lucene.searcher;

import java.util.Comparator;
import org.apache.lucene.search.ScoreDoc;
import org.jetbrains.annotations.Nullable;

public record CurrentPageInfo(@Nullable ScoreDoc last, long remainingLimit, int pageIndex) {

	public static final Comparator<ScoreDoc> TIE_BREAKER = Comparator
			.<ScoreDoc>comparingInt((d) -> d.shardIndex)
			.thenComparingInt(d -> -d.doc);
	public static final CurrentPageInfo EMPTY_STATUS = new CurrentPageInfo(null, 0, 0);
}
