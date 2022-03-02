package it.cavallium.dbengine.database.remote;

import it.cavallium.data.generator.DataSerializer;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.jetbrains.annotations.NotNull;
import org.rocksdb.ColumnFamilyHandle;

public class ColumnFamilyHandleSerializer implements DataSerializer<ColumnFamilyHandle> {

	@Override
	public void serialize(DataOutput dataOutput, @NotNull ColumnFamilyHandle columnFamilyHandle) throws IOException {
		throw new UnsupportedOperationException("Can't encode this type");
	}

	@Override
	public @NotNull ColumnFamilyHandle deserialize(DataInput dataInput) throws IOException {
		throw new UnsupportedOperationException("Can't encode this type");
	}
}
