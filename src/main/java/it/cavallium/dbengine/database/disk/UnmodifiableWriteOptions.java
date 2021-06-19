package it.cavallium.dbengine.database.disk;

import org.rocksdb.AbstractSlice;
import org.rocksdb.AbstractTableFilter;
import org.rocksdb.ReadOptions;
import org.rocksdb.ReadTier;
import org.rocksdb.Snapshot;
import org.rocksdb.WriteOptions;

public class UnmodifiableWriteOptions extends WriteOptions {

	public UnmodifiableWriteOptions() {

	}

	public UnmodifiableWriteOptions(WriteOptions writeOptions) {
		super(writeOptions);
	}

	@Override
	public WriteOptions setDisableWAL(boolean b) {
		throw uoe();
	}

	@Override
	public WriteOptions setIgnoreMissingColumnFamilies(boolean b) {
		throw uoe();
	}

	@Override
	public WriteOptions setLowPri(boolean b) {
		throw uoe();
	}

	@Override
	public WriteOptions setNoSlowdown(boolean b) {
		throw uoe();
	}

	@Override
	public WriteOptions setSync(boolean b) {
		throw uoe();
	}

	private UnsupportedOperationException uoe() {
		return new UnsupportedOperationException("Unmodifiable read options");
	}
}
