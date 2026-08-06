package it.cavallium.dbengine.database.remote;

import it.cavallium.datagen.DataCodec;
import it.cavallium.datagen.ProjectionReadSupport;
import it.cavallium.dbengine.client.Compression;
import it.cavallium.stream.SafeDataInput;
import it.cavallium.stream.SafeDataOutput;
import org.jetbrains.annotations.NotNull;

public class CompressionSerializer implements DataCodec<Compression> {

	@Override
	public void serialize(SafeDataOutput dataOutput, @NotNull Compression compression) {
		dataOutput.writeInt(compression.ordinal());
	}

	@Override
	public @NotNull Compression read(SafeDataInput dataInput) {
		return Compression.values()[dataInput.readInt()];
	}

	@Override
	public void skip(SafeDataInput dataInput) {
		ProjectionReadSupport.skipBytes(dataInput, Integer.BYTES);
	}
}
