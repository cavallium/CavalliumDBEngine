package it.cavallium.dbengine.database.disk;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import it.cavallium.dbengine.database.LLEntry;
import it.cavallium.dbengine.database.LLRange;
import java.util.Map;
import java.util.Map.Entry;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;

public class LLLocalGroupedEntryReactiveRocksIterator extends
		LLLocalGroupedReactiveRocksIterator<LLEntry> {

	public LLLocalGroupedEntryReactiveRocksIterator(RocksDB db, ByteBufAllocator alloc, ColumnFamilyHandle cfh,
			int prefixLength,
			LLRange range,
			boolean allowNettyDirect,
			ReadOptions readOptions,
			String debugName) {
		super(db, alloc, cfh, prefixLength, range, allowNettyDirect, readOptions, false, true);
	}

	@Override
	public LLEntry getEntry(ByteBuf key, ByteBuf value) {
		return new LLEntry(key, value);
	}
}
