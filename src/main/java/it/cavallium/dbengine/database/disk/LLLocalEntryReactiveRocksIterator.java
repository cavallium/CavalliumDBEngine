package it.cavallium.dbengine.database.disk;

import io.net5.buffer.api.Buffer;
import io.net5.buffer.api.BufferAllocator;
import io.net5.buffer.api.Send;
import it.cavallium.dbengine.database.LLEntry;
import it.cavallium.dbengine.database.LLRange;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;

public class LLLocalEntryReactiveRocksIterator extends LLLocalReactiveRocksIterator<Send<LLEntry>> {

	public LLLocalEntryReactiveRocksIterator(RocksDB db,
			BufferAllocator alloc,
			ColumnFamilyHandle cfh,
			Send<LLRange> range,
			boolean allowNettyDirect,
			ReadOptions readOptions,
			String debugName) {
		super(db, alloc, cfh, range, allowNettyDirect, readOptions, true, debugName);
	}

	@Override
	public Send<LLEntry> getEntry(Send<Buffer> key, Send<Buffer> value) {
		return LLEntry.of(key, value).send();
	}
}
