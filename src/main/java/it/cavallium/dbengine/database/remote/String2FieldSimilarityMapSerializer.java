package it.cavallium.dbengine.database.remote;

import it.cavallium.data.generator.DataSerializer;
import it.cavallium.dbengine.lucene.analyzer.TextFieldsSimilarity;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.jetbrains.annotations.NotNull;

public class String2FieldSimilarityMapSerializer implements DataSerializer<Map<String, TextFieldsSimilarity>> {

	private static final TextFieldsSimilaritySerializer TEXT_FIELDS_SIMILARITY_SERIALIZER = new TextFieldsSimilaritySerializer();

	@Override
	public void serialize(DataOutput dataOutput, @NotNull Map<String, TextFieldsSimilarity> stringTextFieldsSimilarityMap)
			throws IOException {
		dataOutput.writeInt(stringTextFieldsSimilarityMap.size());
		for (Entry<String, TextFieldsSimilarity> entry : stringTextFieldsSimilarityMap.entrySet()) {
			dataOutput.writeUTF(entry.getKey());
			TEXT_FIELDS_SIMILARITY_SERIALIZER.serialize(dataOutput, entry.getValue());
		}
	}

	@Override
	public @NotNull Map<String, TextFieldsSimilarity> deserialize(DataInput dataInput) throws IOException {
		var size = dataInput.readInt();
		var result = new HashMap<String, TextFieldsSimilarity>(size);
		for (int i = 0; i < size; i++) {
			result.put(dataInput.readUTF(), TEXT_FIELDS_SIMILARITY_SERIALIZER.deserialize(dataInput));
		}
		return Collections.unmodifiableMap(result);
	}
}
