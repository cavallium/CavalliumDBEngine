package it.cavallium.dbengine.database.disk;

import io.netty.buffer.api.Buffer;
import io.netty.buffer.api.BufferAllocator;
import io.netty.buffer.api.Send;
import it.cavallium.dbengine.database.LLEntry;
import it.cavallium.dbengine.database.LLRange;
import java.util.Map;
import java.util.Map.Entry;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;

public class LLLocalGroupedEntryReactiveRocksIterator extends
		LLLocalGroupedReactiveRocksIterator<Send<LLEntry>> {

	public LLLocalGroupedEntryReactiveRocksIterator(RocksDB db, BufferAllocator alloc, ColumnFamilyHandle cfh,
			int prefixLength,
			Send<LLRange> range,
			boolean allowNettyDirect,
			ReadOptions readOptions,
			String debugName) {
		super(db, alloc, cfh, prefixLength, range, allowNettyDirect, readOptions, false, true);
	}

	@Override
	public Send<LLEntry> getEntry(Send<Buffer> key, Send<Buffer> value) {
		return LLEntry.of(key, value).send();
	}
}
