package it.cavallium.dbengine.database.disk;

import it.cavallium.dbengine.database.SafeCloseable;
import it.cavallium.dbengine.database.disk.rocksdb.RocksIteratorObj;
import it.cavallium.dbengine.database.disk.rocksdb.RocksObj;
import org.rocksdb.ReadOptions;

public record RocksIterWithReadOpts(RocksObj<ReadOptions> readOptions, RocksIteratorObj iter) implements SafeCloseable {

	@Override
	public void close() {
		if (readOptions != null) {
			readOptions.close();
		}
		iter.close();
	}
}
