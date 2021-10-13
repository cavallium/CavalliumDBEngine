package it.cavallium.dbengine.lucene;

/**
 * <pre>y = (x * factor) + firstPageLimit</pre>
 */
public class LinearPageLimits implements PageLimits {

	private static final double DEFAULT_FACTOR = 0.5d;

	private final double factor;
	private final double firstPageLimit;
	private final double maxItemsPerPage;

	public LinearPageLimits() {
		this(DEFAULT_FACTOR, DEFAULT_MIN_ITEMS_PER_PAGE);
	}

	public LinearPageLimits(double factor) {
		this(factor, DEFAULT_MIN_ITEMS_PER_PAGE);
	}

	public LinearPageLimits(double factor, int firstPageLimit) {
		this(factor, firstPageLimit, DEFAULT_MAX_ITEMS_PER_PAGE);
	}

	public LinearPageLimits(double factor, int firstPageLimit, int maxItemsPerPage) {
		this.factor = factor;
		this.firstPageLimit = firstPageLimit;
		this.maxItemsPerPage = maxItemsPerPage;
	}

	@Override
	public int getPageLimit(int pageIndex) {
		double min = Math.min(maxItemsPerPage, firstPageLimit + (pageIndex * factor));
		assert min > 0d;
		return (int) min;
	}
}
