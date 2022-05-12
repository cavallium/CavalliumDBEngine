package it.cavallium.dbengine.database.disk;

import io.netty5.buffer.api.Buffer;
import io.netty5.buffer.api.Send;
import it.cavallium.dbengine.database.LLEntry;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.disk.rocksdb.RocksObj;
import org.rocksdb.ReadOptions;

public class LLLocalGroupedEntryReactiveRocksIterator extends
		LLLocalGroupedReactiveRocksIterator<Send<LLEntry>> {

	public LLLocalGroupedEntryReactiveRocksIterator(RocksDBColumn db,
			int prefixLength,
			Send<LLRange> range,
			boolean allowNettyDirect,
			RocksObj<ReadOptions> readOptions,
			boolean smallRange) {
		super(db, prefixLength, range, allowNettyDirect, readOptions, false, true, smallRange);
	}

	@Override
	public Send<LLEntry> getEntry(Send<Buffer> key, Send<Buffer> value) {
		return LLEntry.of(key, value).send();
	}
}
