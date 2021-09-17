package it.cavallium.dbengine.netty;

import io.net5.buffer.api.pool.BufferAllocatorMetric;
import io.net5.buffer.api.pool.PooledBufferAllocator;
import io.net5.buffer.api.pool.PooledBufferAllocatorMetricUtils;
import java.lang.reflect.Field;

public class JMXPooledNettyMonitoring extends JMXNettyMonitoring implements JMXNettyMonitoringMBean {

	private final PooledBufferAllocator alloc;
	private final BufferAllocatorMetric metric;
	private PooledBufferAllocatorMetricUtils metricUtils;

	public JMXPooledNettyMonitoring(String name, PooledBufferAllocator alloc) {
		super(name, alloc.isDirectBufferPooled(), alloc.metric());
		this.alloc = alloc;
		this.metric = alloc.metric();
		try {
			this.metricUtils = new PooledBufferAllocatorMetricUtils(alloc);
		} catch (Throwable e) {
			this.metricUtils = null;
		}
	}

	@Override
	public Integer getNumHeapArenas() {
		return direct ? 0 : alloc.numArenas();
	}

	@Override
	public Integer getNumDirectArenas() {
		return direct ? alloc.numArenas() : 0;
	}

	@Override
	public Integer getNumThreadLocalCachesArenas() {
		return metricUtils != null ? metricUtils.numThreadLocalCaches() : 0;
	}

	@Deprecated
	@Override
	public Integer getTinyCacheSize() {
		return 0;
	}

	@Override
	public Integer getSmallCacheSize() {
		return metricUtils != null ? metricUtils.smallCacheSize() : 0;
	}

	@Override
	public Integer getNormalCacheSize() {
		return metricUtils != null ? metricUtils.normalCacheSize() : 0;
	}

	@Override
	public Integer getChunkSize() {
		return metricUtils != null ? metricUtils.chunkSize() : 0;
	}
}
