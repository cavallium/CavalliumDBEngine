package it.cavallium.dbengine.database.remote;

import it.cavallium.data.generator.DataSerializer;
import it.cavallium.dbengine.lucene.analyzer.TextFieldsAnalyzer;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.jetbrains.annotations.NotNull;

public class TextFieldsAnalyzerSerializer implements DataSerializer<TextFieldsAnalyzer> {

	@Override
	public void serialize(DataOutput dataOutput, @NotNull TextFieldsAnalyzer textFieldsAnalyzer) {
		dataOutput.writeInt(textFieldsAnalyzer.ordinal());
	}

	@Override
	public @NotNull TextFieldsAnalyzer deserialize(DataInput dataInput) {
		return TextFieldsAnalyzer.values()[dataInput.readInt()];
	}
}
