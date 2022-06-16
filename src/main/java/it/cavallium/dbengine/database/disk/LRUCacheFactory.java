package it.cavallium.dbengine.database.disk;

import org.rocksdb.Cache;
import org.rocksdb.ClockCache;
import org.rocksdb.LRUCache;

public class LRUCacheFactory implements CacheFactory {

	@Override
	public Cache newCache(long size) {
		return new LRUCache(size);
	}
}
