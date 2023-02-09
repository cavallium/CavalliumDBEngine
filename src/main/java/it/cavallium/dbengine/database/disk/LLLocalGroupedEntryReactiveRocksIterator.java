package it.cavallium.dbengine.database.disk;

import it.cavallium.dbengine.buffers.Buf;
import it.cavallium.dbengine.database.LLEntry;
import it.cavallium.dbengine.database.LLRange;
import java.util.function.Supplier;
import org.rocksdb.ReadOptions;

public class LLLocalGroupedEntryReactiveRocksIterator extends LLLocalGroupedReactiveRocksIterator<LLEntry> {

	public LLLocalGroupedEntryReactiveRocksIterator(RocksDBColumn db,
			int prefixLength,
			LLRange range,
			Supplier<ReadOptions> readOptions,
			boolean smallRange) {
		super(db, prefixLength, range, readOptions, true, true, smallRange);
	}

	@Override
	public LLEntry getEntry(Buf key, Buf value) {
		assert key != null;
		assert value != null;
		return LLEntry.of(key.copy(), value.copy());
	}
}
