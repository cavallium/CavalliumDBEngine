package it.cavallium.dbengine.database.disk;

import it.cavallium.dbengine.database.LLRange;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;

public class LLLocalLuceneKeysReactiveIterator extends LLLocalLuceneReactiveIterator<byte[]> {

	public LLLocalLuceneKeysReactiveIterator(RocksDB db,
			ColumnFamilyHandle cfh,
			LLRange range,
			ReadOptions readOptions) {
		super(db, cfh, range, readOptions, false);
	}

	@Override
	public byte[] getEntry(byte[] key, byte[] value) {
		return key;
	}
}
