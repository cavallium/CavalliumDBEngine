package it.cavallium.dbengine.database.collections;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.serialization.Serializer;
import java.util.Map;
import java.util.Map.Entry;
import org.jetbrains.annotations.NotNull;

class ValueWithHashSerializer<X, Y> implements Serializer<Entry<X, Y>, ByteBuf> {

	private final ByteBufAllocator allocator;
	private final Serializer<X, ByteBuf> keySuffixSerializer;
	private final Serializer<Y, ByteBuf> valueSerializer;

	ValueWithHashSerializer(ByteBufAllocator allocator,
			Serializer<X, ByteBuf> keySuffixSerializer,
			Serializer<Y, ByteBuf> valueSerializer) {
		this.allocator = allocator;
		this.keySuffixSerializer = keySuffixSerializer;
		this.valueSerializer = valueSerializer;
	}

	@Override
	public @NotNull Entry<X, Y> deserialize(@NotNull ByteBuf serialized) {
		try {
			X deserializedKey = keySuffixSerializer.deserialize(serialized.retain());
			Y deserializedValue = valueSerializer.deserialize(serialized.retain());
			return Map.entry(deserializedKey, deserializedValue);
		} finally {
			serialized.release();
		}
	}

	@Override
	public @NotNull ByteBuf serialize(@NotNull Entry<X, Y> deserialized) {
		ByteBuf keySuffix = keySuffixSerializer.serialize(deserialized.getKey());
		try {
			ByteBuf value = valueSerializer.serialize(deserialized.getValue());
			try {
				return LLUtils.compositeBuffer(allocator, keySuffix.retain(), value.retain());
			} finally {
				value.release();
			}
		} finally {
			keySuffix.release();
		}
	}
}
