package it.cavallium.dbengine.database.disk;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import it.cavallium.dbengine.database.LLRange;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;

public class LLLocalKeyReactiveRocksIterator extends LLLocalReactiveRocksIterator<ByteBuf> {

	public LLLocalKeyReactiveRocksIterator(RocksDB db,
			ByteBufAllocator alloc,
			ColumnFamilyHandle cfh,
			LLRange range,
			ReadOptions readOptions) {
		super(db, alloc, cfh, range, readOptions, false);
	}

	@Override
	public ByteBuf getEntry(ByteBuf key, ByteBuf value) {
		return key;
	}
}
