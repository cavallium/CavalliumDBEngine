package it.cavallium.dbengine.database.disk;

import it.cavallium.buffer.Buf;
import it.cavallium.dbengine.database.LLRange;
import java.util.function.Supplier;
import org.rocksdb.ReadOptions;

public class LLLocalGroupedKeyReactiveRocksIterator extends LLLocalGroupedReactiveRocksIterator<Buf> {

	public LLLocalGroupedKeyReactiveRocksIterator(RocksDBColumn db,
			int prefixLength,
			LLRange range,
			Supplier<ReadOptions> readOptions,
			boolean smallRange) {
		super(db, prefixLength, range, readOptions, true, false, smallRange);
	}

	@Override
	public Buf getEntry(Buf key, Buf value) {
		assert key != null;
		return key.copy();
	}
}
