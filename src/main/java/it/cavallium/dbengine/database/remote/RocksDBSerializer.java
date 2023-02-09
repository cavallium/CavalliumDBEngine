package it.cavallium.dbengine.database.remote;

import it.cavallium.data.generator.DataSerializer;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.jetbrains.annotations.NotNull;
import org.rocksdb.RocksDB;

public class RocksDBSerializer implements DataSerializer<RocksDB> {

	@Override
	public void serialize(DataOutput dataOutput, @NotNull RocksDB rocksDB) {
		throw new UnsupportedOperationException("Can't encode this type");
	}

	@Override
	public @NotNull RocksDB deserialize(DataInput dataInput) {
		throw new UnsupportedOperationException("Can't encode this type");
	}
}
