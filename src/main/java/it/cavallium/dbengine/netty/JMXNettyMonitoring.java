package it.cavallium.dbengine.netty;

import io.net5.buffer.api.pool.BufferAllocatorMetric;

public class JMXNettyMonitoring implements JMXNettyMonitoringMBean {

	private final String name;
	protected final boolean direct;
	private final BufferAllocatorMetric metric;

	public JMXNettyMonitoring(String name, boolean direct, BufferAllocatorMetric metric) {
		this.name = name;
		this.direct = direct;
		this.metric = metric;
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public Long getHeapUsed() {
		return direct ? 0 : metric.usedMemory();
	}

	@Override
	public Long getDirectUsed() {
		return direct ? metric.usedMemory() : 0;
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
