package it.cavallium.dbengine.database.collections;

import io.netty.buffer.api.Buffer;
import io.netty.buffer.api.BufferAllocator;
import io.netty.buffer.api.Send;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.serialization.SerializationException;
import it.cavallium.dbengine.database.serialization.Serializer;
import java.util.Map;
import java.util.Map.Entry;
import org.jetbrains.annotations.NotNull;

class ValueWithHashSerializer<X, Y> implements Serializer<Entry<X, Y>> {

	private final BufferAllocator allocator;
	private final Serializer<X> keySuffixSerializer;
	private final Serializer<Y> valueSerializer;

	ValueWithHashSerializer(BufferAllocator allocator,
			Serializer<X> keySuffixSerializer,
			Serializer<Y> valueSerializer) {
		this.allocator = allocator;
		this.keySuffixSerializer = keySuffixSerializer;
		this.valueSerializer = valueSerializer;
	}

	@Override
	public @NotNull DeserializationResult<Entry<X, Y>> deserialize(@NotNull Send<Buffer> serializedToReceive)
			throws SerializationException {
		try (var serialized = serializedToReceive.receive()) {
			DeserializationResult<X> deserializedKey = keySuffixSerializer.deserialize(serialized.copy().send());
			DeserializationResult<Y> deserializedValue = valueSerializer.deserialize(serialized.send());
			return new DeserializationResult<>(Map.entry(deserializedKey.deserializedData(),
					deserializedValue.deserializedData()), deserializedKey.bytesRead() + deserializedValue.bytesRead());
		}
	}

	@Override
	public @NotNull Send<Buffer> serialize(@NotNull Entry<X, Y> deserialized) throws SerializationException {
		try (Buffer keySuffix = keySuffixSerializer.serialize(deserialized.getKey()).receive()) {
			try (Buffer value = valueSerializer.serialize(deserialized.getValue()).receive()) {
				return LLUtils.compositeBuffer(allocator, keySuffix.send(), value.send());
			}
		}
	}
}
