package it.cavallium.dbengine.lucene.searcher;

import java.util.Comparator;
import org.apache.lucene.search.ScoreDoc;

public record PaginationInfo(long totalLimit, long firstPageOffset, long firstPageLimit, boolean forceSinglePage) {

	public static final int MAX_SINGLE_SEARCH_LIMIT = 256;
	public static final int FIRST_PAGE_LIMIT = 10;
	/**
	 * Use true to allow a custom unscored collector when possible
	 */
	public static final boolean ALLOW_UNSCORED_PAGINATION_MODE = true;
}
