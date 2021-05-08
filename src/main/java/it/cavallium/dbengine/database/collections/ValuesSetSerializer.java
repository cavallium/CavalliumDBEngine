package it.cavallium.dbengine.database.collections;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import it.cavallium.dbengine.database.serialization.Serializer;
import it.unimi.dsi.fastutil.objects.ObjectArraySet;
import java.util.Set;
import org.jetbrains.annotations.NotNull;

class ValuesSetSerializer<X> implements Serializer<Set<X>, ByteBuf> {

	private final ByteBufAllocator allocator;
	private final Serializer<X, ByteBuf> entrySerializer;

	ValuesSetSerializer(ByteBufAllocator allocator, Serializer<X, ByteBuf> entrySerializer) {
		this.allocator = allocator;
		this.entrySerializer = entrySerializer;
	}

	@Override
	public @NotNull Set<X> deserialize(@NotNull ByteBuf serialized) {
		try {
			int entriesLength = serialized.readInt();
			Object[] values = new Object[entriesLength];
			for (int i = 0; i < entriesLength; i++) {
				X entry = entrySerializer.deserialize(serialized.retain());
				values[i] = entry;
			}
			return new ObjectArraySet<>(values);
		} finally {
			serialized.release();
		}
	}

	@Override
	public @NotNull ByteBuf serialize(@NotNull Set<X> deserialized) {
		ByteBuf output = allocator.buffer();
		try {
			output.writeInt(deserialized.size());
			deserialized.forEach((entry) -> {
				ByteBuf serialized = entrySerializer.serialize(entry);
				try {
					output.writeBytes(serialized);
				} finally {
					serialized.release();
				}
			});
			return output.retain();
		} finally {
			output.release();
		}
	}
}
