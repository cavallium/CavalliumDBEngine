package it.cavallium.dbengine.database.remote;

import it.cavallium.data.generator.DataSerializer;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import org.jetbrains.annotations.NotNull;

@SuppressWarnings("rawtypes")
public class StringEntrySerializer implements DataSerializer<Map.Entry> {

	@Override
	public void serialize(DataOutput dataOutput, @NotNull Map.Entry entry) throws IOException {
		dataOutput.writeUTF((String) entry.getKey());
		dataOutput.writeUTF((String) entry.getValue());
	}

	@Override
	public @NotNull Map.Entry deserialize(DataInput dataInput) throws IOException {
		return Map.entry(dataInput.readUTF(), dataInput.readUTF());
	}
}
