package it.cavallium.dbengine.database.remote;

import it.cavallium.data.generator.DataSerializer;
import it.cavallium.stream.SafeDataInput;
import it.cavallium.stream.SafeDataOutput;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.jetbrains.annotations.NotNull;
import org.rocksdb.ColumnFamilyHandle;

public class ColumnFamilyHandleSerializer implements DataSerializer<ColumnFamilyHandle> {

	@Override
	public void serialize(SafeDataOutput dataOutput, @NotNull ColumnFamilyHandle columnFamilyHandle) {
		throw new UnsupportedOperationException("Can't encode this type");
	}

	@Override
	public @NotNull ColumnFamilyHandle deserialize(SafeDataInput dataInput) {
		throw new UnsupportedOperationException("Can't encode this type");
	}
}
