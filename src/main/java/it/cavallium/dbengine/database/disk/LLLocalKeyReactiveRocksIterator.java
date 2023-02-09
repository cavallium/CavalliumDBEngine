package it.cavallium.dbengine.database.disk;

import it.cavallium.dbengine.buffers.Buf;
import it.cavallium.dbengine.database.LLRange;
import java.util.function.Supplier;
import org.rocksdb.ReadOptions;

public class LLLocalKeyReactiveRocksIterator extends LLLocalReactiveRocksIterator<Buf> {

	public LLLocalKeyReactiveRocksIterator(RocksDBColumn db,
			LLRange rangeMono,
			Supplier<ReadOptions> readOptions,
			boolean reverse,
			boolean smallRange) {
		super(db, rangeMono, readOptions, false, reverse, smallRange);
	}

	@Override
	public Buf getEntry(Buf key, Buf value) {
		return key != null ? key.copy() : null;
	}
}
