package it.cavallium.dbengine.database.collections;

import io.net5.buffer.api.Buffer;
import io.net5.buffer.api.BufferAllocator;
import io.net5.buffer.api.Send;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.serialization.SerializationException;
import it.cavallium.dbengine.database.serialization.Serializer;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

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
	public @NotNull DeserializationResult<Entry<X, Y>> deserialize(@Nullable Send<Buffer> serializedToReceive)
			throws SerializationException {
		Objects.requireNonNull(serializedToReceive);
		try (var serialized = serializedToReceive.receive()) {
			DeserializationResult<X> deserializedKey = keySuffixSerializer.deserialize(serialized.copy().send());
			DeserializationResult<Y> deserializedValue = valueSerializer.deserialize(serialized
					.copy(serialized.readerOffset() + deserializedKey.bytesRead(),
							serialized.readableBytes() - deserializedKey.bytesRead()
					)
					.send());
			return new DeserializationResult<>(Map.entry(deserializedKey.deserializedData(),
					deserializedValue.deserializedData()), deserializedKey.bytesRead() + deserializedValue.bytesRead());
		}
	}

	@Override
	public @Nullable Send<Buffer> serialize(@NotNull Entry<X, Y> deserialized) throws SerializationException {
		var keySuffix = keySuffixSerializer.serialize(deserialized.getKey());
		var value = valueSerializer.serialize(deserialized.getValue());
		if (value == null && keySuffix == null) {
			return null;
		} else if (value == null) {
			return keySuffix;
		} else if (keySuffix == null) {
			return value;
		} else {
			return LLUtils.compositeBuffer(allocator, keySuffix, value).send();
		}
	}
}
