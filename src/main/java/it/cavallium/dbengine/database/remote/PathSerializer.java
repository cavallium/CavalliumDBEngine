package it.cavallium.dbengine.database.remote;

import it.cavallium.data.generator.DataSerializer;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.file.Path;
import org.jetbrains.annotations.NotNull;

public class PathSerializer implements DataSerializer<Path> {

	@Override
	public void serialize(DataOutput dataOutput, @NotNull Path path) throws IOException {
		dataOutput.writeUTF(path.toString());
	}

	@Override
	public @NotNull Path deserialize(DataInput dataInput) throws IOException {
		return Path.of(dataInput.readUTF());
	}
}
