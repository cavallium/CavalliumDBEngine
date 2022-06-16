package it.cavallium.dbengine.database.disk;

import org.rocksdb.Cache;

public interface CacheFactory {

	Cache newCache(long size);
}
