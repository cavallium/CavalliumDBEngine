package it.cavallium.dbengine.database.disk;

import it.cavallium.dbengine.database.LLRange;
import java.util.Map;
import java.util.Map.Entry;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;

public class LLLocalLuceneEntryReactiveIterator extends LLLocalLuceneReactiveIterator<Entry<byte[], byte[]>> {

	public LLLocalLuceneEntryReactiveIterator(RocksDB db,
			ColumnFamilyHandle cfh,
			LLRange range,
			ReadOptions readOptions) {
		super(db, cfh, range, readOptions, true);
	}

	@Override
	public Entry<byte[], byte[]> getEntry(byte[] key, byte[] value) {
		return Map.entry(key, value);
	}
}
