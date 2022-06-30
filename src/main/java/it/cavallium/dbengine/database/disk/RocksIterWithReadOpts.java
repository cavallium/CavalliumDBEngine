package it.cavallium.dbengine.database.disk;

import it.cavallium.dbengine.database.DiscardingCloseable;
import it.cavallium.dbengine.database.SafeCloseable;
import it.cavallium.dbengine.database.disk.rocksdb.RocksIteratorObj;
import org.rocksdb.ReadOptions;

public record RocksIterWithReadOpts(ReadOptions readOptions, RocksIteratorObj iter) implements DiscardingCloseable {

	@Override
	public void close() {
		if (readOptions != null && readOptions.isAccessible()) {
			readOptions.close();
		}
		iter.close();
	}
}
