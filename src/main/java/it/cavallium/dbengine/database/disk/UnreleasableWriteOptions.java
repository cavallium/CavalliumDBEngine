package it.cavallium.dbengine.database.disk;

import org.rocksdb.WriteOptions;

public class UnreleasableWriteOptions extends WriteOptions {

	public UnreleasableWriteOptions() {

	}

	public UnreleasableWriteOptions(WriteOptions writeOptions) {
		super(writeOptions);
	}

	@Override
	public void close() {
		throw new UnsupportedOperationException("Can't close " + this.getClass().getSimpleName());
	}
}
