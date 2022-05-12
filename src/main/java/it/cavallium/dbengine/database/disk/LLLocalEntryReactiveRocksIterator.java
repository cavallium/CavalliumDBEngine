package it.cavallium.dbengine.database.disk;

import io.netty5.buffer.api.Buffer;
import io.netty5.buffer.api.Send;
import it.cavallium.dbengine.database.LLEntry;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.disk.rocksdb.RocksObj;
import org.rocksdb.ReadOptions;

public class LLLocalEntryReactiveRocksIterator extends LLLocalReactiveRocksIterator<Send<LLEntry>> {

	public LLLocalEntryReactiveRocksIterator(RocksDBColumn db,
			Send<LLRange> range,
			boolean allowNettyDirect,
			RocksObj<ReadOptions> readOptions,
			boolean reverse,
			boolean smallRange) {
		super(db, range, allowNettyDirect, readOptions, true, reverse, smallRange);
	}

	@Override
	public Send<LLEntry> getEntry(Send<Buffer> key, Send<Buffer> value) {
		return LLEntry.of(key, value).send();
	}
}
