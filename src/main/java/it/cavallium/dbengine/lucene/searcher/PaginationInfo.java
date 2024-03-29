package it.cavallium.dbengine.lucene.searcher;

import it.cavallium.dbengine.lucene.PageLimits;

public record PaginationInfo(long totalLimit, long firstPageOffset, PageLimits pageLimits, boolean forceSinglePage) {

	public static final int MAX_SINGLE_SEARCH_LIMIT = 256;
	public static final int FIRST_PAGE_LIMIT = 10;
	/**
	 * Use true to allow a custom unscored collector when possible
	 */
	public static final boolean ALLOW_UNSCORED_PAGINATION_MODE = true;
}
