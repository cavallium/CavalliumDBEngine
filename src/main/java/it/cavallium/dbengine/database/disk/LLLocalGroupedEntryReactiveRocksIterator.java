package it.cavallium.dbengine.database.disk;

import it.cavallium.buffer.Buf;
import it.cavallium.dbengine.database.LLEntry;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.disk.rocksdb.LLReadOptions;
import java.util.function.Supplier;
import org.rocksdb.ReadOptions;

public class LLLocalGroupedEntryReactiveRocksIterator extends LLLocalGroupedReactiveRocksIterator<LLEntry> {

	public LLLocalGroupedEntryReactiveRocksIterator(RocksDBColumn db,
			int prefixLength,
			LLRange range,
			Supplier<LLReadOptions> readOptions,
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
