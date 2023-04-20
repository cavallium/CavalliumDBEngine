package it.cavallium.dbengine.database.remote;

import it.cavallium.datagen.DataSerializer;
import it.cavallium.dbengine.lucene.analyzer.TextFieldsSimilarity;
import it.cavallium.stream.SafeDataInput;
import it.cavallium.stream.SafeDataOutput;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.jetbrains.annotations.NotNull;

public class TextFieldsSimilaritySerializer implements DataSerializer<TextFieldsSimilarity> {

	@Override
	public void serialize(SafeDataOutput dataOutput, @NotNull TextFieldsSimilarity textFieldsSimilarity) {
		dataOutput.writeInt(textFieldsSimilarity.ordinal());
	}

	@Override
	public @NotNull TextFieldsSimilarity deserialize(SafeDataInput dataInput) {
		return TextFieldsSimilarity.values()[dataInput.readInt()];
	}
}
