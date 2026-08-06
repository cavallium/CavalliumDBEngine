package it.cavallium.dbengine.database.remote;

import it.cavallium.datagen.DataCodec;
import it.cavallium.stream.SafeDataInput;
import it.cavallium.stream.SafeDataOutput;
import org.jetbrains.annotations.NotNull;
import org.rocksdb.ColumnFamilyHandle;

public class ColumnFamilyHandleSerializer implements DataCodec<ColumnFamilyHandle> {

	@Override
	public void serialize(SafeDataOutput dataOutput, @NotNull ColumnFamilyHandle columnFamilyHandle) {
		throw new UnsupportedOperationException("Can't encode this type");
	}

	@Override
	public @NotNull ColumnFamilyHandle read(SafeDataInput dataInput) {
		throw new UnsupportedOperationException("Can't encode this type");
	}

	@Override
	public void skip(SafeDataInput dataInput) {
		throw new UnsupportedOperationException("Can't encode this type");
	}
}
