package it.cavallium.dbengine.netty;

import io.netty5.buffer.api.pool.BufferAllocatorMetric;
import io.netty5.buffer.api.pool.PooledBufferAllocator;
import java.lang.reflect.Field;

public class JMXPooledNettyMonitoring extends JMXNettyMonitoring implements JMXNettyMonitoringMBean {

	private final PooledBufferAllocator alloc;
	private final BufferAllocatorMetric metric;

	public JMXPooledNettyMonitoring(String name, PooledBufferAllocator alloc) {
		super(name, alloc.isDirectBufferPooled(), alloc.metric());
		this.alloc = alloc;
		this.metric = alloc.metric();
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
		return 0;
	}

	@Deprecated
	@Override
	public Integer getTinyCacheSize() {
		return 0;
	}

	@Override
	public Integer getSmallCacheSize() {
		return 0;
	}

	@Override
	public Integer getNormalCacheSize() {
		return 0;
	}

	@Override
	public Integer getChunkSize() {
		return 0;
	}
}
