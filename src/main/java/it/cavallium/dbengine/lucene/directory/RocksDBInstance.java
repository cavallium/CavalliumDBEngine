package it.cavallium.dbengine.lucene.directory;

import java.util.Map;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;

public record RocksDBInstance(RocksDB db, Map<String, ColumnFamilyHandle> handles) {}
