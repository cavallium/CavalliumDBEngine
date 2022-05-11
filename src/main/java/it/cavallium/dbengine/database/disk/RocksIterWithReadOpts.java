package it.cavallium.dbengine.database.disk;

import it.cavallium.dbengine.database.SafeCloseable;
import org.rocksdb.ReadOptions;

record RocksIterWithReadOpts(ReadOptions readOptions, RocksIteratorTuple iter) implements SafeCloseable {

	@Override
	public void close() {
		if (readOptions != null) {
			readOptions.close();
		}
		iter.close();
	}
}
