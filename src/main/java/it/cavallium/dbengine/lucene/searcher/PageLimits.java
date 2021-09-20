package it.cavallium.dbengine.lucene.searcher;

import it.cavallium.dbengine.lucene.LuceneUtils;

public interface PageLimits {

	int DEFAULT_MIN_ITEMS_PER_PAGE = 10;
	int DEFAULT_MAX_ITEMS_PER_PAGE = 500;

	int getPageLimit(int pageIndex);
}
