package it.cavallium.dbengine.database.remote;

import it.cavallium.datagen.DataCodec;
import it.cavallium.stream.SafeDataInput;
import it.cavallium.stream.SafeDataOutput;
import java.util.Map;
import org.jetbrains.annotations.NotNull;
import org.rocksdb.ColumnFamilyHandle;

public class String2ColumnFamilyHandleMapSerializer implements DataCodec<Map<String, ColumnFamilyHandle>> {

	@Override
	public void serialize(SafeDataOutput dataOutput, @NotNull Map<String, ColumnFamilyHandle> stringColumnFamilyHandleMap) {
		throw new UnsupportedOperationException("Can't encode this type");
	}

	@Override
	public @NotNull Map<String, ColumnFamilyHandle> read(SafeDataInput dataInput) {
		throw new UnsupportedOperationException("Can't encode this type");
	}

	@Override
	public void skip(SafeDataInput dataInput) {
		throw new UnsupportedOperationException("Can't encode this type");
	}
}
