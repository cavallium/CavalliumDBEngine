package it.cavallium.dbengine;

import it.cavallium.dbengine.client.MultiSort;
import it.cavallium.dbengine.client.SearchResultKey;
import it.cavallium.dbengine.client.query.BasicType;

record ExpectedQueryType(boolean shard, boolean sorted, boolean sortedByScore, boolean complete, boolean onlyCount) {

	public ExpectedQueryType(boolean shard, MultiSort<SearchResultKey<String>> multiSort, boolean complete, boolean onlyCount) {
		this(shard, multiSort.isSorted(), multiSort.getQuerySort().getBasicType$() == BasicType.ScoreSort, complete, onlyCount);
	}
}
