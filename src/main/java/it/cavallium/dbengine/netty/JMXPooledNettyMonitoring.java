package it.cavallium.dbengine.netty;

import io.netty5.buffer.api.pool.BufferAllocatorMetric;
import io.netty5.buffer.api.pool.PooledBufferAllocator;
import java.lang.reflect.Field;

public class JMXPooledNettyMonitoring extends JMXNettyMonitoring implements JMXNettyMonitoringMBean {

	private final PooledBufferAllocator alloc;
	private final BufferAllocatorMetric metric;
	private Field smallCacheSize;
	private Field numThreadLocalCaches;
	private Field normalCacheSize;
	private Field chunkSize;

	public JMXPooledNettyMonitoring(String name, PooledBufferAllocator alloc) {
		super(name, alloc.isDirectBufferPooled(), alloc.metric());
		this.alloc = alloc;
		this.metric = alloc.metric();
		try {
			//noinspection JavaReflectionMemberAccess
			numThreadLocalCaches = metric.getClass().getDeclaredField("numThreadLocalCaches");
		} catch (NoSuchFieldException e) {
		}
		try {
			//noinspection JavaReflectionMemberAccess
			smallCacheSize = metric.getClass().getDeclaredField("smallCacheSize");
		} catch (NoSuchFieldException e) {
		}
		try {
			//noinspection JavaReflectionMemberAccess
			normalCacheSize = metric.getClass().getDeclaredField("normalCacheSize");
		} catch (NoSuchFieldException e) {
		}
		try {
			//noinspection JavaReflectionMemberAccess
			chunkSize = metric.getClass().getDeclaredField("chunkSize");
		} catch (NoSuchFieldException e) {
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
		try {
			return numThreadLocalCaches.getInt(metric);
		} catch (IllegalAccessException e) {
			return 0;
		}
	}

	@Deprecated
	@Override
	public Integer getTinyCacheSize() {
		return 0;
	}

	@Override
	public Integer getSmallCacheSize() {
		try {
			return smallCacheSize.getInt(metric);
		} catch (IllegalAccessException e) {
			return 0;
		}
	}

	@Override
	public Integer getNormalCacheSize() {
		try {
			return normalCacheSize.getInt(metric);
		} catch (IllegalAccessException e) {
			return 0;
		}
	}

	@Override
	public Integer getChunkSize() {
		try {
			return chunkSize.getInt(metric);
		} catch (IllegalAccessException e) {
			return 0;
		}
	}
}
