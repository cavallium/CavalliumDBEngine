package it.cavallium.dbengine.database.disk;

import org.rocksdb.Cache;
import org.rocksdb.HyperClockCache;

public class HyperClockCacheFactory implements CacheFactory {

	@Override
	public Cache newCache(long size) {
		return new HyperClockCache(size, 0, -1, false);
	}
}
