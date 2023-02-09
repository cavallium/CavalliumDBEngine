package it.cavallium.dbengine.database.remote;

import it.cavallium.data.generator.DataSerializer;
import it.cavallium.dbengine.client.Compression;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.jetbrains.annotations.NotNull;

public class CompressionSerializer implements DataSerializer<Compression> {

	@Override
	public void serialize(DataOutput dataOutput, @NotNull Compression compression) {
		dataOutput.writeInt(compression.ordinal());
	}

	@Override
	public @NotNull Compression deserialize(DataInput dataInput) {
		return Compression.values()[dataInput.readInt()];
	}
}
