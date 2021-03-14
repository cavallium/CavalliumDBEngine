package it.cavallium.dbengine.database.disk;

import it.cavallium.dbengine.database.LLRange;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;

public class LLLocalLuceneGroupedKeysReactiveIterator extends LLLocalLuceneGroupedReactiveIterator<byte[]> {

	public LLLocalLuceneGroupedKeysReactiveIterator(RocksDB db,
			ColumnFamilyHandle cfh,
			int prefixLength,
			LLRange range,
			ReadOptions readOptions,
			String debugName) {
		super(db, cfh, prefixLength, range, readOptions, false, debugName);
	}

	@Override
	public byte[] getEntry(byte[] key, byte[] value) {
		return key;
	}
}
