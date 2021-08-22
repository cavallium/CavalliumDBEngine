package it.cavallium.dbengine.database.disk;

import org.rocksdb.*;

public class UnreleasableReadOptions extends ReadOptions {

	public UnreleasableReadOptions() {

	}

	public UnreleasableReadOptions(ReadOptions readOptions) {
		super(readOptions);
	}

	@Override
	public void close() {
		throw new UnsupportedOperationException("Can't close " + this.getClass().getSimpleName());
	}
}
