package it.cavallium.dbengine.database.disk;

import io.netty.buffer.api.Buffer;
import io.netty.buffer.api.BufferAllocator;
import io.netty.buffer.api.Send;
import it.cavallium.dbengine.database.LLRange;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;

public class LLLocalGroupedKeyReactiveRocksIterator extends LLLocalGroupedReactiveRocksIterator<Send<Buffer>> {

	public LLLocalGroupedKeyReactiveRocksIterator(RocksDB db,
			BufferAllocator alloc,
			ColumnFamilyHandle cfh,
			int prefixLength,
			Send<LLRange> range,
			boolean allowNettyDirect,
			ReadOptions readOptions,
			String debugName) {
		super(db, alloc, cfh, prefixLength, range, allowNettyDirect, readOptions, true, false);
	}

	@Override
	public Send<Buffer> getEntry(Send<Buffer> key, Send<Buffer> value) {
		if (value != null) {
			value.close();
		}
		return key;
	}
}
