package it.cavallium.dbengine.database.remote;

import it.cavallium.datagen.DataCodec;
import it.cavallium.datagen.ProjectionReadSupport;
import it.cavallium.stream.SafeDataInput;
import it.cavallium.stream.SafeDataOutput;
import java.nio.file.Path;
import org.jetbrains.annotations.NotNull;

public class PathSerializer implements DataCodec<Path> {

	@Override
	public void serialize(SafeDataOutput dataOutput, @NotNull Path path) {
		dataOutput.writeUTF(path.toString());
	}

	@Override
	public @NotNull Path read(SafeDataInput dataInput) {
		return Path.of(dataInput.readUTF());
	}

	@Override
	public void skip(SafeDataInput dataInput) {
		ProjectionReadSupport.skipPayload(dataInput, dataInput.readUnsignedShort());
	}
}
