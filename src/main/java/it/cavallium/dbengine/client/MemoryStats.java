package it.cavallium.dbengine.client;

public record MemoryStats(long estimateTableReadersMem, long sizeAllMemTables,
													long curSizeAllMemTables, long estimateNumKeys, long blockCacheCapacity,
													long blockCacheUsage, long blockCachePinnedUsage, long liveVersions) {}
