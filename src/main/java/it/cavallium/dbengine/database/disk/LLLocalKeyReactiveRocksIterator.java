package it.cavallium.dbengine.database.disk;

import io.netty5.buffer.api.Buffer;
import io.netty5.buffer.api.Send;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.disk.rocksdb.RocksObj;
import org.rocksdb.ReadOptions;

public class LLLocalKeyReactiveRocksIterator extends LLLocalReactiveRocksIterator<Send<Buffer>> {

	public LLLocalKeyReactiveRocksIterator(RocksDBColumn db,
			Send<LLRange> range,
			boolean allowNettyDirect,
			RocksObj<ReadOptions> readOptions,
			boolean reverse,
			boolean smallRange) {
		super(db, range, allowNettyDirect, readOptions, false, reverse, smallRange);
	}

	@Override
	public Send<Buffer> getEntry(Send<Buffer> key, Send<Buffer> value) {
		if (value != null) {
			value.close();
		}
		return key;
	}
}
