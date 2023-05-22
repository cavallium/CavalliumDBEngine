package it.cavallium.dbengine.database.disk;

import it.cavallium.buffer.Buf;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.disk.rocksdb.LLReadOptions;
import java.util.function.Supplier;
import org.rocksdb.ReadOptions;

public class LLLocalKeyReactiveRocksIterator extends LLLocalReactiveRocksIterator<Buf> {

	public LLLocalKeyReactiveRocksIterator(RocksDBColumn db,
			LLRange rangeMono,
			Supplier<LLReadOptions> readOptions,
			boolean reverse,
			boolean smallRange) {
		super(db, rangeMono, readOptions, false, reverse, smallRange);
	}

	@Override
	public Buf getEntry(Buf key, Buf value) {
		return key != null ? key.copy() : null;
	}
}
