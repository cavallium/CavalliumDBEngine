package it.cavallium.dbengine.database.collections;

import it.cavallium.dbengine.buffers.BufDataInput;
import it.cavallium.dbengine.buffers.BufDataOutput;
import it.cavallium.dbengine.database.serialization.SerializationException;
import it.cavallium.dbengine.database.serialization.Serializer;
import it.unimi.dsi.fastutil.objects.ObjectArraySet;
import java.util.ArrayList;
import java.util.Objects;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

class ValuesSetSerializer<X> implements Serializer<ObjectArraySet<X>> {

	private static final Logger logger = LogManager.getLogger(ValuesSetSerializer.class);

	private final Serializer<X> entrySerializer;

	ValuesSetSerializer(Serializer<X> entrySerializer) {
		this.entrySerializer = entrySerializer;
	}

	@Override
	public @NotNull ObjectArraySet<X> deserialize(@NotNull BufDataInput in) throws SerializationException {
		try {
			Objects.requireNonNull(in);
			if (in.available() <= 0) {
				logger.error("Can't deserialize, 0 bytes are readable");
				return new ObjectArraySet<>();
			}
			int entriesLength = in.readInt();
			ArrayList<X> deserializedElements = new ArrayList<>(entriesLength);
			for (int i = 0; i < entriesLength; i++) {
				var deserializationResult = entrySerializer.deserialize(in);
				deserializedElements.add(deserializationResult);
			}
			return new ObjectArraySet<>(deserializedElements);
		} catch (IndexOutOfBoundsException ex) {
			logger.error("Error during deserialization of value set, returning an empty set", ex);
			return new ObjectArraySet<>();
		}
	}

	@Override
	public void serialize(@NotNull ObjectArraySet<X> deserialized, BufDataOutput out) throws SerializationException {
		out.writeInt(deserialized.size());
		for (X entry : deserialized) {
			entrySerializer.serialize(entry, out);
		}
	}

	@Override
	public int getSerializedSizeHint() {
		return -1;
	}
}
