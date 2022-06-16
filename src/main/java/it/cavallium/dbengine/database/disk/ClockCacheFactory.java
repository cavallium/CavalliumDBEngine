package it.cavallium.dbengine.database.disk;

import org.rocksdb.Cache;
import org.rocksdb.ClockCache;

public class ClockCacheFactory implements CacheFactory {

	@Override
	public Cache newCache(long size) {
		return new ClockCache(size);
	}
}
