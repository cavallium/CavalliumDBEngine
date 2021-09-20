package it.cavallium.dbengine.lucene.searcher;

import it.cavallium.dbengine.lucene.LuceneUtils;
import java.util.Comparator;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.ScoreDoc;
import org.jetbrains.annotations.Nullable;

record CurrentPageInfo(@Nullable ScoreDoc last, long remainingLimit, int pageIndex) {

	public static final Comparator<ScoreDoc> TIE_BREAKER = Comparator.comparingInt((d) -> d.shardIndex);
	public static final CurrentPageInfo EMPTY_STATUS = new CurrentPageInfo(null, 0, 0);
}
