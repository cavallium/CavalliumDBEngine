package it.cavallium.dbengine.database.collections;

import io.netty5.buffer.api.Buffer;
import io.netty5.buffer.api.BufferAllocator;
import io.netty5.buffer.api.Send;
import it.cavallium.dbengine.database.serialization.SerializationException;
import it.cavallium.dbengine.database.serialization.Serializer;
import it.unimi.dsi.fastutil.objects.ObjectArraySet;
import java.util.ArrayList;
import org.jetbrains.annotations.NotNull;

class ValuesSetSerializer<X> implements Serializer<ObjectArraySet<X>> {

	private final BufferAllocator allocator;
	private final Serializer<X> entrySerializer;

	ValuesSetSerializer(BufferAllocator allocator, Serializer<X> entrySerializer) {
		this.allocator = allocator;
		this.entrySerializer = entrySerializer;
	}

	@Override
	public @NotNull DeserializationResult<ObjectArraySet<X>> deserialize(@NotNull Send<Buffer> serializedToReceive) throws SerializationException {
		try (var serialized = serializedToReceive.receive()) {
			int initialReaderOffset = serialized.readerOffset();
			int entriesLength = serialized.readInt();
			ArrayList<X> deserializedElements = new ArrayList<>(entriesLength);
			for (int i = 0; i < entriesLength; i++) {
				var deserializationResult = entrySerializer.deserialize(serialized
						.copy(serialized.readerOffset(), serialized.readableBytes())
						.send());
				deserializedElements.add(deserializationResult.deserializedData());
				serialized.readerOffset(serialized.readerOffset() + deserializationResult.bytesRead());
			}
			return new DeserializationResult<>(new ObjectArraySet<>(deserializedElements), serialized.readerOffset() - initialReaderOffset);
		}
	}

	@Override
	public @NotNull Send<Buffer> serialize(@NotNull ObjectArraySet<X> deserialized) throws SerializationException {
		try (Buffer output = allocator.allocate(64)) {
			output.writeInt(deserialized.size());
			for (X entry : deserialized) {
				try (Buffer serialized = entrySerializer.serialize(entry).receive()) {
					output.ensureWritable(serialized.readableBytes());
					output.writeBytes(serialized);
				}
			}
			return output.send();
		}
	}
}
