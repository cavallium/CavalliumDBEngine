package it.cavallium.dbengine.database.serialization;

import it.cavallium.buffer.BufDataInput;
import it.cavallium.buffer.BufDataOutput;
import java.io.IOException;
import org.jetbrains.annotations.NotNull;

public interface Codec<A> {

	@NotNull A deserialize(@NotNull BufDataInput serialized);

	void serialize(@NotNull BufDataOutput outputStream, @NotNull A deserialized);
}
