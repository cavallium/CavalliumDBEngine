package it.cavallium.dbengine.database.collections;

import io.netty.buffer.api.Buffer;
import io.netty.buffer.api.BufferAllocator;
import io.netty.buffer.api.Send;
import it.cavallium.dbengine.database.serialization.SerializationException;
import it.cavallium.dbengine.database.serialization.Serializer;
import it.unimi.dsi.fastutil.objects.ObjectArraySet;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectSets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.jetbrains.annotations.NotNull;

class ValuesSetSerializer<X> implements Serializer<ObjectArraySet<X>, Send<Buffer>> {

	private final BufferAllocator allocator;
	private final Serializer<X, Send<Buffer>> entrySerializer;

	ValuesSetSerializer(BufferAllocator allocator, Serializer<X, Send<Buffer>> entrySerializer) {
		this.allocator = allocator;
		this.entrySerializer = entrySerializer;
	}

	@Override
	public @NotNull ObjectArraySet<X> deserialize(@NotNull Send<Buffer> serializedToReceive) throws SerializationException {
		try (var serialized = serializedToReceive.receive()) {
			int entriesLength = serialized.readInt();
			ArrayList<X> deserializedElements = new ArrayList<>(entriesLength);
			for (int i = 0; i < entriesLength; i++) {
				X entry = entrySerializer.deserialize(serialized.send());
				deserializedElements.add(entry);
			}
			return new ObjectArraySet<>(deserializedElements);
		}
	}

	@Override
	public @NotNull Send<Buffer> serialize(@NotNull ObjectArraySet<X> deserialized) throws SerializationException {
		try (Buffer output = allocator.allocate(64)) {
			output.writeInt(deserialized.size());
			for (X entry : deserialized) {
				try (Buffer serialized = entrySerializer.serialize(entry).receive()) {
					output.writeBytes(serialized);
				}
			}
			return output.send();
		}
	}
}
