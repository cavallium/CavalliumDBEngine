package it.cavallium.dbengine.netty;

public interface JMXPoolArenaNettyMonitoringMBean {

	Integer getNumThreadCaches();

	Integer getNumTinySubpages();

	Integer getNumSmallSubpages();

	Integer getNumChunkLists();

	Long getNumAllocations();

	Long getNumTinyAllocations();

	Long getNumSmallAllocations();

	Long getNumNormalAllocations();

	Long getNumHugeAllocations();

	Long getNumDeallocations();

	Long getNumTinyDeallocations();

	Long getNumSmallDeallocations();

	Long getNumNormalDeallocations();

	Long getNumHugeDeallocations();

	Long getNumActiveAllocations();

	Long getNumActiveTinyAllocations();

	Long getNumActiveSmallAllocations();

	Long getNumActiveNormalAllocations();

	Long getNumActiveHugeAllocations();

	Long getNumActiveBytes();
}
