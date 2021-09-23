package it.cavallium.dbengine.lucene.searcher;

import it.cavallium.dbengine.lucene.LuceneUtils;

/**
 * <pre>y = 2 ^ (x + pageIndexOffset) + firstPageLimit</pre>
 */
public class ExponentialPageLimits implements PageLimits {

	private static final int DEFAULT_PAGE_INDEX_OFFSET = 0;

	private final int pageIndexOffset;
	private final int firstPageLimit;
	private final int maxItemsPerPage;

	public ExponentialPageLimits() {
		this(DEFAULT_PAGE_INDEX_OFFSET);
	}

	public ExponentialPageLimits(int pageIndexOffset) {
		this(pageIndexOffset, DEFAULT_MIN_ITEMS_PER_PAGE);
	}

	public ExponentialPageLimits(int pageIndexOffset, int firstPageLimit) {
		this(pageIndexOffset, firstPageLimit, DEFAULT_MAX_ITEMS_PER_PAGE);
	}

	public ExponentialPageLimits(int pageIndexOffset, int firstPageLimit, int maxItemsPerPage) {
		this.pageIndexOffset = pageIndexOffset;
		this.firstPageLimit = firstPageLimit;
		this.maxItemsPerPage = maxItemsPerPage;
	}

	@Override
	public int getPageLimit(int pageIndex) {
		var offsetedIndex = pageIndex + pageIndexOffset;
		var power = 0b1L << offsetedIndex;

		if (offsetedIndex >= 30) { // safety
			return maxItemsPerPage;
		}

		var min = Math.max(firstPageLimit, Math.min(maxItemsPerPage, firstPageLimit + power));
		assert min > 0;
		return LuceneUtils.safeLongToInt(min);
	}
}
