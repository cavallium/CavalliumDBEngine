package it.cavallium.dbengine.database.remote;

import it.cavallium.datagen.DataCodec;
import it.cavallium.datagen.ProjectionReadSupport;
import it.cavallium.stream.SafeDataInput;
import it.cavallium.stream.SafeDataOutput;
import java.util.Map;
import org.jetbrains.annotations.NotNull;

@SuppressWarnings("rawtypes")
public class StringEntrySerializer implements DataCodec<Map.Entry> {

	@Override
	public void serialize(SafeDataOutput dataOutput, @NotNull Map.Entry entry) {
		dataOutput.writeUTF((String) entry.getKey());
		dataOutput.writeUTF((String) entry.getValue());
	}

	@Override
	public @NotNull Map.Entry read(SafeDataInput dataInput) {
		return Map.entry(dataInput.readUTF(), dataInput.readUTF());
	}

	@Override
	public void skip(SafeDataInput dataInput) {
		ProjectionReadSupport.skipPayload(dataInput, dataInput.readUnsignedShort());
		ProjectionReadSupport.skipPayload(dataInput, dataInput.readUnsignedShort());
	}
}
