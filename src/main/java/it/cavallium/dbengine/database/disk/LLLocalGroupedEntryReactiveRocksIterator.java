package it.cavallium.dbengine.database.disk;

import io.netty5.buffer.api.Buffer;
import io.netty5.util.Send;
import it.cavallium.dbengine.database.LLEntry;
import it.cavallium.dbengine.database.LLRange;
import org.rocksdb.ReadOptions;

public class LLLocalGroupedEntryReactiveRocksIterator extends
		LLLocalGroupedReactiveRocksIterator<LLEntry> {

	public LLLocalGroupedEntryReactiveRocksIterator(RocksDBColumn db,
			int prefixLength,
			LLRange range,
			boolean allowNettyDirect,
			ReadOptions readOptions,
			boolean smallRange) {
		super(db, prefixLength, range, allowNettyDirect, readOptions, false, true, smallRange);
	}

	@Override
	public LLEntry getEntry(Buffer key, Buffer value) {
		assert key != null;
		assert value != null;
		return LLEntry.of(
				key.touch("iteration entry key"),
				value.touch("iteration entry value")
		);
	}
}
