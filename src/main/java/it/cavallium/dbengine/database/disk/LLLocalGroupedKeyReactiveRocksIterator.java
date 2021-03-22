package it.cavallium.dbengine.database.disk;

import it.cavallium.dbengine.database.LLRange;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;

public class LLLocalGroupedKeyReactiveRocksIterator extends LLLocalGroupedReactiveRocksIterator<byte[]> {

	public LLLocalGroupedKeyReactiveRocksIterator(RocksDB db,
			ColumnFamilyHandle cfh,
			int prefixLength,
			LLRange range,
			ReadOptions readOptions,
			String debugName) {
		super(db, cfh, prefixLength, range, readOptions, true, false, debugName);
	}

	@Override
	public byte[] getEntry(byte[] key, byte[] value) {
		return key;
	}
}
