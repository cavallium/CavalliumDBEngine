package it.cavallium.dbengine.database.remote;

import it.cavallium.datagen.DataSerializer;
import it.cavallium.dbengine.lucene.analyzer.TextFieldsAnalyzer;
import it.cavallium.stream.SafeDataInput;
import it.cavallium.stream.SafeDataOutput;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.jetbrains.annotations.NotNull;

public class TextFieldsAnalyzerSerializer implements DataSerializer<TextFieldsAnalyzer> {

	@Override
	public void serialize(SafeDataOutput dataOutput, @NotNull TextFieldsAnalyzer textFieldsAnalyzer) {
		dataOutput.writeInt(textFieldsAnalyzer.ordinal());
	}

	@Override
	public @NotNull TextFieldsAnalyzer deserialize(SafeDataInput dataInput) {
		return TextFieldsAnalyzer.values()[dataInput.readInt()];
	}
}
