package it.cavallium.dbengine.database.remote;

import it.cavallium.data.generator.DataSerializer;
import it.cavallium.dbengine.lucene.analyzer.TextFieldsSimilarity;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.jetbrains.annotations.NotNull;

public class TextFieldsSimilaritySerializer implements DataSerializer<TextFieldsSimilarity> {

	@Override
	public void serialize(DataOutput dataOutput, @NotNull TextFieldsSimilarity textFieldsSimilarity) {
		dataOutput.writeInt(textFieldsSimilarity.ordinal());
	}

	@Override
	public @NotNull TextFieldsSimilarity deserialize(DataInput dataInput) {
		return TextFieldsSimilarity.values()[dataInput.readInt()];
	}
}
