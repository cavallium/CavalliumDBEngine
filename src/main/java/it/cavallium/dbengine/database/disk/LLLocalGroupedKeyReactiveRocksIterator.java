package it.cavallium.dbengine.database.disk;

import io.netty5.buffer.api.Buffer;
import io.netty5.buffer.api.Send;
import it.cavallium.dbengine.database.LLRange;
import org.rocksdb.ReadOptions;

public class LLLocalGroupedKeyReactiveRocksIterator extends LLLocalGroupedReactiveRocksIterator<Buffer> {

	public LLLocalGroupedKeyReactiveRocksIterator(RocksDBColumn db,
			int prefixLength,
			LLRange range,
			boolean allowNettyDirect,
			ReadOptions readOptions,
			boolean smallRange) {
		super(db, prefixLength, range, allowNettyDirect, readOptions, true, false, smallRange);
	}

	@Override
	public Buffer getEntry(Buffer key, Buffer value) {
		if (value != null) {
			value.close();
		}
		return key;
	}
}
