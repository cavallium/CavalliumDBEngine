package it.cavallium.dbengine.database.remote;

import it.cavallium.datagen.DataCodec;
import it.cavallium.datagen.ProjectionReadSupport;
import it.cavallium.stream.SafeDataInput;
import it.cavallium.stream.SafeDataOutput;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.jetbrains.annotations.NotNull;

public class StringMapSerializer implements DataCodec<Map<String, String>> {

	@Override
	public void serialize(SafeDataOutput dataOutput, @NotNull Map<String, String> stringTextFieldsAnalyzerMap) {
		dataOutput.writeInt(stringTextFieldsAnalyzerMap.size());
		for (Entry<String, String> entry : stringTextFieldsAnalyzerMap.entrySet()) {
			dataOutput.writeUTF(entry.getKey());
			dataOutput.writeUTF(entry.getValue());
		}
	}

	@Override
	public @NotNull Map<String, String> read(SafeDataInput dataInput) {
		var size = ProjectionReadSupport.readLength(dataInput);
		ProjectionReadSupport.prepareArrayAllocation(dataInput, size, 2 * Short.BYTES);
		var result = new HashMap<String, String>(size);
		for (int i = 0; i < size; i++) {
			result.put(dataInput.readUTF(), dataInput.readUTF());
		}
		return Collections.unmodifiableMap(result);
	}

	@Override
	public void skip(SafeDataInput dataInput) {
		int size = ProjectionReadSupport.readLength(dataInput);
		ProjectionReadSupport.prepareArrayAllocation(dataInput, size, 2 * Short.BYTES);
		for (int i = 0; i < size; i++) {
			ProjectionReadSupport.skipPayload(dataInput, dataInput.readUnsignedShort());
			ProjectionReadSupport.skipPayload(dataInput, dataInput.readUnsignedShort());
		}
	}
}
