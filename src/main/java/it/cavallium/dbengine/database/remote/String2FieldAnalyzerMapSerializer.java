package it.cavallium.dbengine.database.remote;

import it.cavallium.datagen.DataSerializer;
import it.cavallium.dbengine.lucene.analyzer.TextFieldsAnalyzer;
import it.cavallium.stream.SafeDataInput;
import it.cavallium.stream.SafeDataOutput;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.jetbrains.annotations.NotNull;

public class String2FieldAnalyzerMapSerializer implements DataSerializer<Map<String, TextFieldsAnalyzer>> {

	private static final TextFieldsAnalyzerSerializer TEXT_FIELDS_ANALYZER_SERIALIZER = new TextFieldsAnalyzerSerializer();

	@Override
	public void serialize(SafeDataOutput dataOutput, @NotNull Map<String, TextFieldsAnalyzer> stringTextFieldsAnalyzerMap) {
		dataOutput.writeInt(stringTextFieldsAnalyzerMap.size());
		for (Entry<String, TextFieldsAnalyzer> entry : stringTextFieldsAnalyzerMap.entrySet()) {
			dataOutput.writeUTF(entry.getKey());
			TEXT_FIELDS_ANALYZER_SERIALIZER.serialize(dataOutput, entry.getValue());
		}
	}

	@Override
	public @NotNull Map<String, TextFieldsAnalyzer> deserialize(SafeDataInput dataInput) {
		var size = dataInput.readInt();
		var result = new HashMap<String, TextFieldsAnalyzer>(size);
		for (int i = 0; i < size; i++) {
			result.put(dataInput.readUTF(), TEXT_FIELDS_ANALYZER_SERIALIZER.deserialize(dataInput));
		}
		return Collections.unmodifiableMap(result);
	}
}
