package it.cavallium.dbengine.database.remote;

import it.cavallium.datagen.DataSerializer;
import it.cavallium.stream.SafeDataInput;
import it.cavallium.stream.SafeDataOutput;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.file.Path;
import org.jetbrains.annotations.NotNull;

public class PathSerializer implements DataSerializer<Path> {

	@Override
	public void serialize(SafeDataOutput dataOutput, @NotNull Path path) {
		dataOutput.writeUTF(path.toString());
	}

	@Override
	public @NotNull Path deserialize(SafeDataInput dataInput) {
		return Path.of(dataInput.readUTF());
	}
}
