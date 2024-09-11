package it.cavallium.dbengine.utils;

/**
 * <pre>y = 2 ^ (x + pageIndexOffset) + firstPageLimit</pre>
 */
public class ExponentialLimits {

	private static final int DEFAULT_PAGE_INDEX_OFFSET = 0;

	private final int pageIndexOffset;
	private final int firstPageLimit;
	private final int maxItemsPerPage;

	public ExponentialLimits(int pageIndexOffset, int firstPageLimit, int maxItemsPerPage) {
		this.pageIndexOffset = pageIndexOffset;
		this.firstPageLimit = firstPageLimit;
		this.maxItemsPerPage = maxItemsPerPage;
	}

	public int getPageLimit(int pageIndex) {
		var offsetedIndex = pageIndex + pageIndexOffset;
		var power = 0b1L << offsetedIndex;

		if (offsetedIndex >= 30) { // safety
			return maxItemsPerPage;
		}

		var min = Math.max(firstPageLimit, Math.min(maxItemsPerPage, firstPageLimit + power));
		assert min > 0;
		return safeLongToInt(min);
	}

	private static int safeLongToInt(long l) {
		if (l > 2147483630) {
			return 2147483630;
		} else if (l < -2147483630) {
			return -2147483630;
		} else {
			return (int) l;
		}
	}
}
