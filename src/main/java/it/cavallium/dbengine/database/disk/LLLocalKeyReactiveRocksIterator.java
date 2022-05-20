package it.cavallium.dbengine.database.disk;

import io.netty5.buffer.api.Buffer;
import io.netty5.buffer.api.Send;
import it.cavallium.dbengine.database.LLRange;
import org.rocksdb.ReadOptions;

public class LLLocalKeyReactiveRocksIterator extends LLLocalReactiveRocksIterator<Buffer> {

	public LLLocalKeyReactiveRocksIterator(RocksDBColumn db,
			LLRange range,
			boolean allowNettyDirect,
			ReadOptions readOptions,
			boolean reverse,
			boolean smallRange) {
		super(db, range, allowNettyDirect, readOptions, false, reverse, smallRange);
	}

	@Override
	public Buffer getEntry(Buffer key, Buffer value) {
		if (value != null) {
			value.close();
		}
		return key;
	}
}
