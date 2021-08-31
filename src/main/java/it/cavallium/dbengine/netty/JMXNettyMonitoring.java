package it.cavallium.dbengine.netty;

public class JMXNettyMonitoring implements JMXNettyMonitoringMBean {

	private final String name;
	protected final boolean direct;
	private final io.netty.buffer.api.pool.BufferAllocatorMetric metric;

	public JMXNettyMonitoring(String name, boolean direct, io.netty.buffer.api.pool.BufferAllocatorMetric metric) {
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
