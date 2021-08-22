package it.cavallium.dbengine.database.collections;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
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

class ValuesSetSerializer<X> implements Serializer<ObjectArraySet<X>, ByteBuf> {

	private final ByteBufAllocator allocator;
	private final Serializer<X, ByteBuf> entrySerializer;

	ValuesSetSerializer(ByteBufAllocator allocator, Serializer<X, ByteBuf> entrySerializer) {
		this.allocator = allocator;
		this.entrySerializer = entrySerializer;
	}

	@Override
	public @NotNull ObjectArraySet<X> deserialize(@NotNull ByteBuf serialized) throws SerializationException {
		try {
			int entriesLength = serialized.readInt();
			ArrayList<X> deserializedElements = new ArrayList<>(entriesLength);
			for (int i = 0; i < entriesLength; i++) {
				X entry = entrySerializer.deserialize(serialized.retain());
				deserializedElements.add(entry);
			}
			return new ObjectArraySet<>(deserializedElements);
		} finally {
			serialized.release();
		}
	}

	@Override
	public @NotNull ByteBuf serialize(@NotNull ObjectArraySet<X> deserialized) throws SerializationException {
		ByteBuf output = allocator.buffer();
		try {
			output.writeInt(deserialized.size());
			for (X entry : deserialized) {
				ByteBuf serialized = entrySerializer.serialize(entry);
				try {
					output.writeBytes(serialized);
				} finally {
					serialized.release();
				}
			}
			return output.retain();
		} finally {
			output.release();
		}
	}
}
