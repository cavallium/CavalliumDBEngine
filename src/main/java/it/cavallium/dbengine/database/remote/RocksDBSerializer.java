package it.cavallium.dbengine.database.remote;

import it.cavallium.datagen.DataCodec;
import it.cavallium.stream.SafeDataInput;
import it.cavallium.stream.SafeDataOutput;
import org.jetbrains.annotations.NotNull;
import org.rocksdb.RocksDB;

public class RocksDBSerializer implements DataCodec<RocksDB> {

	@Override
	public void serialize(SafeDataOutput dataOutput, @NotNull RocksDB rocksDB) {
		throw new UnsupportedOperationException("Can't encode this type");
	}

	@Override
	public @NotNull RocksDB read(SafeDataInput dataInput) {
		throw new UnsupportedOperationException("Can't encode this type");
	}

	@Override
	public void skip(SafeDataInput dataInput) {
		throw new UnsupportedOperationException("Can't encode this type");
	}
}
