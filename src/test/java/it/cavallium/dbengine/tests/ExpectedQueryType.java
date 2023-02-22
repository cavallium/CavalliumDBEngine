package it.cavallium.dbengine.tests;

import it.cavallium.dbengine.client.Sort;
import it.cavallium.dbengine.client.query.BaseType;

record ExpectedQueryType(boolean shard, boolean sorted, boolean sortedByScore, boolean complete, boolean onlyCount) {

	public ExpectedQueryType(boolean shard, Sort multiSort, boolean complete, boolean onlyCount) {
		this(shard, multiSort.isSorted(), multiSort.querySort().getBaseType$() == BaseType.ScoreSort, complete, onlyCount);
	}
}
