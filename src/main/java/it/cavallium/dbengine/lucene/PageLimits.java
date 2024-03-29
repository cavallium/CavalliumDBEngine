package it.cavallium.dbengine.lucene;

public interface PageLimits {

	int DEFAULT_MIN_ITEMS_PER_PAGE = 10;
	int DEFAULT_MAX_ITEMS_PER_PAGE = 250;

	int getPageLimit(int pageIndex);
}
