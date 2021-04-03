package it.cavallium.dbengine.database.disk;

import it.cavallium.dbengine.database.LLRange;
import java.util.Map;
import java.util.Map.Entry;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;

public class LLLocalGroupedEntryReactiveRocksIterator extends
		LLLocalGroupedReactiveRocksIterator<Entry<byte[], byte[]>> {

	public LLLocalGroupedEntryReactiveRocksIterator(RocksDB db,
			ColumnFamilyHandle cfh,
			int prefixLength,
			LLRange range,
			ReadOptions readOptions,
			String debugName) {
		super(db, cfh, prefixLength, range, readOptions, false, true);
	}

	@Override
	public Entry<byte[], byte[]> getEntry(byte[] key, byte[] value) {
		return Map.entry(key, value);
	}
}
