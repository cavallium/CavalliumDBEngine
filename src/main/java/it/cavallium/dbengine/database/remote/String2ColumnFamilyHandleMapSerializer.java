package it.cavallium.dbengine.database.remote;

import it.cavallium.data.generator.DataSerializer;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import org.jetbrains.annotations.NotNull;
import org.rocksdb.ColumnFamilyHandle;

public class String2ColumnFamilyHandleMapSerializer implements DataSerializer<Map<String, ColumnFamilyHandle>> {

	@Override
	public void serialize(DataOutput dataOutput, @NotNull Map<String, ColumnFamilyHandle> stringColumnFamilyHandleMap)
			throws IOException {
		throw new UnsupportedOperationException("Can't encode this type");
	}

	@Override
	public @NotNull Map<String, ColumnFamilyHandle> deserialize(DataInput dataInput) {
		throw new UnsupportedOperationException("Can't encode this type");
	}
}
