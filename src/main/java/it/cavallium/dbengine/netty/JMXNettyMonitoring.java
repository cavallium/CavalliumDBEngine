package it.cavallium.dbengine.netty;

import io.netty.buffer.api.BufferAllocatorMetric;

public class JMXNettyMonitoring implements JMXNettyMonitoringMBean {

	private final String name;
	private final ByteBufAllocatorMetric metric;

	public JMXNettyMonitoring(String name, io.netty.buffer.api.BufferAllocatorMetric metric) {
		this.name = name;
		this.metric = metric;
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public Long getHeapUsed() {
		return metric.usedHeapMemory();
	}

	@Override
	public Long getDirectUsed() {
		return metric.usedDirectMemory();
	}

	@Override
	public Integer getNumHeapArenas() {
		return null;
	}

	@Override
	public Integer getNumDirectArenas() {
		return null;
	}

	@Override
	public Integer getNumThreadLocalCachesArenas() {
		return null;
	}

	@Override
	public Integer getTinyCacheSize() {
		return null;
	}

	@Override
	public Integer getSmallCacheSize() {
		return null;
	}

	@Override
	public Integer getNormalCacheSize() {
		return null;
	}

	@Override
	public Integer getChunkSize() {
		return null;
	}
}
