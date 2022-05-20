package it.cavallium.dbengine.database.disk;

import io.netty5.buffer.api.Buffer;
import io.netty5.buffer.api.Send;
import it.cavallium.dbengine.database.LLEntry;
import it.cavallium.dbengine.database.LLRange;
import org.rocksdb.ReadOptions;

public class LLLocalEntryReactiveRocksIterator extends LLLocalReactiveRocksIterator<LLEntry> {

	public LLLocalEntryReactiveRocksIterator(RocksDBColumn db,
			LLRange range,
			boolean allowNettyDirect,
			ReadOptions readOptions,
			boolean reverse,
			boolean smallRange) {
		super(db, range, allowNettyDirect, readOptions, true, reverse, smallRange);
	}

	@Override
	public LLEntry getEntry(Buffer key, Buffer value) {
		assert key != null;
		assert value != null;
		return LLEntry.of(
				key.touch("iteration entry key"),
				value.touch("iteration entry value")
		);
	}
}
