package it.cavallium.dbengine;

record ExpectedQueryType(boolean shard, boolean sorted, boolean sortedByScore, boolean complete, boolean onlyCount) {}
