package it.cavallium.dbengine.database.collections;

import io.net5.buffer.api.Buffer;
import io.net5.buffer.api.BufferAllocator;
import io.net5.buffer.api.Send;
import it.cavallium.dbengine.database.serialization.SerializationException;
import it.cavallium.dbengine.database.serialization.Serializer;
import it.unimi.dsi.fastutil.objects.ObjectArraySet;
import java.util.ArrayList;
import java.util.Objects;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

class ValuesSetSerializer<X> implements Serializer<ObjectArraySet<X>> {

	private final Serializer<X> entrySerializer;

	ValuesSetSerializer(Serializer<X> entrySerializer) {
		this.entrySerializer = entrySerializer;
	}

	@Override
	public @NotNull ObjectArraySet<X> deserialize(@NotNull Buffer serialized) throws SerializationException {
		Objects.requireNonNull(serialized);
		int entriesLength = serialized.readInt();
		ArrayList<X> deserializedElements = new ArrayList<>(entriesLength);
		for (int i = 0; i < entriesLength; i++) {
			var deserializationResult = entrySerializer.deserialize(serialized);
			deserializedElements.add(deserializationResult);
		}
		return new ObjectArraySet<>(deserializedElements);
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
