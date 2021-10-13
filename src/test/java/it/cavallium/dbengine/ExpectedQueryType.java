package it.cavallium.dbengine;

record ExpectedQueryType(boolean shard, boolean sorted, boolean complete, boolean onlyCount) {}
