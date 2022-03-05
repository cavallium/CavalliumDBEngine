package it.cavallium.dbengine.database.remote;

import it.cavallium.data.generator.DataSerializer;
import it.cavallium.dbengine.lucene.analyzer.TextFieldsAnalyzer;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.jetbrains.annotations.NotNull;

public class StringMapSerializer implements DataSerializer<Map<String, String>> {

	@Override
	public void serialize(DataOutput dataOutput, @NotNull Map<String, String> stringTextFieldsAnalyzerMap)
			throws IOException {
		dataOutput.writeInt(stringTextFieldsAnalyzerMap.size());
		for (Entry<String, String> entry : stringTextFieldsAnalyzerMap.entrySet()) {
			dataOutput.writeUTF(entry.getKey());
			dataOutput.writeUTF(entry.getValue());
		}
	}

	@Override
	public @NotNull Map<String, String> deserialize(DataInput dataInput) throws IOException {
		var size = dataInput.readInt();
		var result = new HashMap<String, String>(size);
		for (int i = 0; i < size; i++) {
			result.put(dataInput.readUTF(), dataInput.readUTF());
		}
		return Collections.unmodifiableMap(result);
	}
}