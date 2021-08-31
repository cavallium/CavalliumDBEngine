package it.cavallium.dbengine.database.serialization;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.jetbrains.annotations.NotNull;

public interface Codec<A> {

	@NotNull A deserialize(@NotNull BufferDataInput serialized) throws IOException;

	void serialize(@NotNull BufferDataOutput outputStream, @NotNull A deserialized) throws IOException;
}
