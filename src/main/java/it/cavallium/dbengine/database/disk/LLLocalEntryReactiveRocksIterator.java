package it.cavallium.dbengine.database.disk;

import it.cavallium.dbengine.buffers.Buf;
import it.cavallium.dbengine.database.LLEntry;
import it.cavallium.dbengine.database.LLRange;
import java.util.function.Supplier;
import org.rocksdb.ReadOptions;

public class LLLocalEntryReactiveRocksIterator extends LLLocalReactiveRocksIterator<LLEntry> {

	public LLLocalEntryReactiveRocksIterator(RocksDBColumn db,
			LLRange range,
			Supplier<ReadOptions> readOptions,
			boolean reverse,
			boolean smallRange) {
		super(db, range, readOptions, true, reverse, smallRange);
	}

	@Override
	public LLEntry getEntry(Buf key, Buf value) {
		assert key != null;
		assert value != null;
		return LLEntry.of(key.copy(), value.copy());
	}
}
