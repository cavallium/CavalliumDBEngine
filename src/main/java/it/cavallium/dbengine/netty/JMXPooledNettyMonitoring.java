package it.cavallium.dbengine.netty;

import io.netty.buffer.api.BufferAllocatorMetric;
import io.netty.buffer.PooledByteBufAllocatorMetric;

public class JMXPooledNettyMonitoring extends JMXNettyMonitoring implements JMXNettyMonitoringMBean {

	private final PooledByteBufAllocatorMetric metric;

	public JMXPooledNettyMonitoring(String name, PooledByteBufAllocatorMetric metric) {
		super(name, metric);
		this.metric = metric;
	}

	@Override
	public Integer getNumHeapArenas() {
		return metric.numHeapArenas();
	}

	@Override
	public Integer getNumDirectArenas() {
		return metric.numDirectArenas();
	}

	@Override
	public Integer getNumThreadLocalCachesArenas() {
		return metric.numThreadLocalCaches();
	}

	@Deprecated
	@Override
	public Integer getTinyCacheSize() {
		return metric.tinyCacheSize();
	}

	@Override
	public Integer getSmallCacheSize() {
		return metric.smallCacheSize();
	}

	@Override
	public Integer getNormalCacheSize() {
		return metric.normalCacheSize();
	}

	@Override
	public Integer getChunkSize() {
		return metric.chunkSize();
	}
}
