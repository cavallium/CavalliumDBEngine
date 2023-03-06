package it.cavallium.dbengine.database.remote;

import it.cavallium.data.generator.DataSerializer;
import it.cavallium.stream.SafeDataInput;
import it.cavallium.stream.SafeDataOutput;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import org.jetbrains.annotations.NotNull;

@SuppressWarnings("rawtypes")
public class StringEntrySerializer implements DataSerializer<Map.Entry> {

	@Override
	public void serialize(SafeDataOutput dataOutput, @NotNull Map.Entry entry) {
		dataOutput.writeUTF((String) entry.getKey());
		dataOutput.writeUTF((String) entry.getValue());
	}

	@Override
	public @NotNull Map.Entry deserialize(SafeDataInput dataInput) {
		return Map.entry(dataInput.readUTF(), dataInput.readUTF());
	}
}
