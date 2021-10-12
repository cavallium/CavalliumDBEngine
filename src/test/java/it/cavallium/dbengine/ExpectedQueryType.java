package it.cavallium.dbengine;

record ExpectedQueryType(boolean shard, boolean sorted, boolean scored, boolean unlimited, boolean onlyCount) {}
