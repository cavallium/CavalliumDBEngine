package it.cavallium.dbengine.lucene.searcher;

public class SinglePageLimits implements PageLimits {

	private final int firstPageLimit;

	public SinglePageLimits() {
		this(DEFAULT_MIN_ITEMS_PER_PAGE);
	}

	public SinglePageLimits(int firstPageLimit) {
		this.firstPageLimit = firstPageLimit;
	}

	@Override
	public int getPageLimit(int pageIndex) {
		if (pageIndex == 0) {
			return firstPageLimit;
		} else {
			return 0;
		}
	}
}
