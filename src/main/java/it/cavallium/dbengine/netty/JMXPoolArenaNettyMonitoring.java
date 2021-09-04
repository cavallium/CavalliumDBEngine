package it.cavallium.dbengine.netty;

import io.netty5.buffer.PoolArenaMetric;

public class JMXPoolArenaNettyMonitoring implements JMXPoolArenaNettyMonitoringMBean {

	private final PoolArenaMetric metric;

	public JMXPoolArenaNettyMonitoring(PoolArenaMetric metric) {
		this.metric = metric;
	}

	@Override
	public Integer getNumThreadCaches() {
		return metric.numThreadCaches();
	}

	@Deprecated
	@Override
	public Integer getNumTinySubpages() {
		return metric.numTinySubpages();
	}

	@Override
	public Integer getNumSmallSubpages() {
		return metric.numSmallSubpages();
	}

	@Override
	public Integer getNumChunkLists() {
		return metric.numChunkLists();
	}

	@Override
	public Long getNumAllocations() {
		return metric.numAllocations();
	}

	@Override
	public Long getNumTinyAllocations() {
		return metric.numTinyAllocations();
	}

	@Override
	public Long getNumSmallAllocations() {
		return metric.numSmallAllocations();
	}

	@Override
	public Long getNumNormalAllocations() {
		return metric.numNormalAllocations();
	}

	@Override
	public Long getNumHugeAllocations() {
		return metric.numHugeAllocations();
	}

	@Override
	public Long getNumDeallocations() {
		return metric.numDeallocations();
	}

	@Override
	public Long getNumTinyDeallocations() {
		return metric.numTinyDeallocations();
	}

	@Override
	public Long getNumSmallDeallocations() {
		return metric.numSmallDeallocations();
	}

	@Override
	public Long getNumNormalDeallocations() {
		return metric.numNormalDeallocations();
	}

	@Override
	public Long getNumHugeDeallocations() {
		return metric.numHugeDeallocations();
	}

	@Override
	public Long getNumActiveAllocations() {
		return metric.numActiveAllocations();
	}

	@Deprecated
	@Override
	public Long getNumActiveTinyAllocations() {
		return metric.numActiveTinyAllocations();
	}

	@Override
	public Long getNumActiveSmallAllocations() {
		return metric.numActiveSmallAllocations();
	}

	@Override
	public Long getNumActiveNormalAllocations() {
		return metric.numActiveNormalAllocations();
	}

	@Override
	public Long getNumActiveHugeAllocations() {
		return metric.numActiveHugeAllocations();
	}

	@Override
	public Long getNumActiveBytes() {
		return metric.numActiveBytes();
	}
}
