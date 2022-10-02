package it.cavallium.dbengine.database.collections;

import io.netty5.buffer.Buffer;
import io.netty5.buffer.BufferAllocator;
import io.netty5.util.Send;
import it.cavallium.dbengine.database.serialization.SerializationException;
import it.cavallium.dbengine.database.serialization.Serializer;
import it.unimi.dsi.fastutil.objects.ObjectArraySet;
import java.util.ArrayList;
import java.util.Objects;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

class ValuesSetSerializer<X> implements Serializer<ObjectArraySet<X>> {

	private static final Logger logger = LogManager.getLogger(ValuesSetSerializer.class);

	private final Serializer<X> entrySerializer;

	ValuesSetSerializer(Serializer<X> entrySerializer) {
		this.entrySerializer = entrySerializer;
	}

	@Override
	public @NotNull ObjectArraySet<X> deserialize(@NotNull Buffer serialized) throws SerializationException {
		try {
			Objects.requireNonNull(serialized);
			if (serialized.readableBytes() == 0) {
				logger.error("Can't deserialize, 0 bytes are readable");
				return new ObjectArraySet<>();
			}
			int entriesLength = serialized.readInt();
			ArrayList<X> deserializedElements = new ArrayList<>(entriesLength);
			for (int i = 0; i < entriesLength; i++) {
				var deserializationResult = entrySerializer.deserialize(serialized);
				deserializedElements.add(deserializationResult);
			}
			return new ObjectArraySet<>(deserializedElements);
		} catch (IndexOutOfBoundsException ex) {
			logger.error("Error during deserialization of value set, returning an empty set", ex);
			return new ObjectArraySet<>();
		}
	}

	@Override
	public void serialize(@NotNull ObjectArraySet<X> deserialized, Buffer output) throws SerializationException {
		output.writeInt(deserialized.size());
		for (X entry : deserialized) {
			entrySerializer.serialize(entry, output);
		}
	}

	@Override
	public int getSerializedSizeHint() {
		return -1;
	}
}
