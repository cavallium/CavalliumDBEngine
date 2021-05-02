package it.cavallium.dbengine.database.disk;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import it.cavallium.dbengine.database.LLRange;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;

public class LLLocalGroupedKeyReactiveRocksIterator extends LLLocalGroupedReactiveRocksIterator<ByteBuf> {

	public LLLocalGroupedKeyReactiveRocksIterator(RocksDB db,
			ByteBufAllocator alloc,
			ColumnFamilyHandle cfh,
			int prefixLength,
			LLRange range,
			ReadOptions readOptions,
			String debugName) {
		super(db, alloc, cfh, prefixLength, range, readOptions, true, false);
	}

	@Override
	public ByteBuf getEntry(ByteBuf key, ByteBuf value) {
		if (value != null) {
			value.release();
		}
		return key;
	}
}
