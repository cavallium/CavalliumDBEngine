package it.cavallium.dbengine.lucene.searcher;

import it.cavallium.dbengine.lucene.LuceneUtils;

public class ExponentialPageLimits implements PageLimits {

	private final int firstPageLimit;
	private final int maxItemsPerPage;

	public ExponentialPageLimits() {
		this(DEFAULT_MIN_ITEMS_PER_PAGE);
	}

	public ExponentialPageLimits(int firstPageLimit) {
		this(firstPageLimit, DEFAULT_MAX_ITEMS_PER_PAGE);
	}

	public ExponentialPageLimits(int firstPageLimit, int maxItemsPerPage) {
		this.firstPageLimit = firstPageLimit;
		this.maxItemsPerPage = maxItemsPerPage;
	}

	@Override
	public int getPageLimit(int pageIndex) {
		if (pageIndex >= 10) { // safety
			return maxItemsPerPage;
		}
		var limitedPageIndex = Math.max(1, pageIndex);
		var min = Math.max(firstPageLimit, Math.min(maxItemsPerPage,
				LuceneUtils.safeLongToInt(limitedPageIndex * (0b1L << limitedPageIndex))));
		assert min > 0;
		return min;
	}
}
