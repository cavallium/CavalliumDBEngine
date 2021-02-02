package it.cavallium.dbengine.database.serialization;

import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import java.io.IOException;
import org.jetbrains.annotations.NotNull;

public interface Codec<A> {

	@NotNull A deserialize(@NotNull ByteBufInputStream serialized) throws IOException;

	void serialize(@NotNull ByteBufOutputStream outputStream, @NotNull A deserialized) throws IOException;
}
