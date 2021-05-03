package it.cavallium.dbengine.netty;

public interface JMXNettyMonitoringMBean {

	String getName();

	Long getHeapUsed();

	Long getDirectUsed();

	Integer getNumHeapArenas();

	Integer getNumDirectArenas();

	Integer getNumThreadLocalCachesArenas();

	Integer getTinyCacheSize();

	Integer getSmallCacheSize();

	Integer getNormalCacheSize();

	Integer getChunkSize();
}
