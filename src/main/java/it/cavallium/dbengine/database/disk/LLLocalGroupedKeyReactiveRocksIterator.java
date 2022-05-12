package it.cavallium.dbengine.database.disk;

import io.netty5.buffer.api.Buffer;
import io.netty5.buffer.api.Send;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.disk.rocksdb.RocksObj;
import org.rocksdb.ReadOptions;

public class LLLocalGroupedKeyReactiveRocksIterator extends LLLocalGroupedReactiveRocksIterator<Send<Buffer>> {

	public LLLocalGroupedKeyReactiveRocksIterator(RocksDBColumn db,
			int prefixLength,
			Send<LLRange> range,
			boolean allowNettyDirect,
			RocksObj<ReadOptions> readOptions,
			boolean smallRange) {
		super(db, prefixLength, range, allowNettyDirect, readOptions, true, false, smallRange);
	}

	@Override
	public Send<Buffer> getEntry(Send<Buffer> key, Send<Buffer> value) {
		if (value != null) {
			value.close();
		}
		return key;
	}
}
