package it.cavallium.dbengine.client;

import io.net5.buffer.api.Buffer;
import io.net5.buffer.api.Send;
import it.cavallium.dbengine.database.serialization.SerializationException;
import it.cavallium.dbengine.database.serialization.SerializerFixedBinaryLength;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class MappedSerializerFixedLength<A, B> implements SerializerFixedBinaryLength<B> {

	private final SerializerFixedBinaryLength<A> fixedLengthSerializer;
	private final Mapper<A, B> keyMapper;

	public MappedSerializerFixedLength(SerializerFixedBinaryLength<A> fixedLengthSerializer,
			Mapper<A, B> keyMapper) {
		this.fixedLengthSerializer = fixedLengthSerializer;
		this.keyMapper = keyMapper;
	}

	@Override
	public @NotNull DeserializationResult<B> deserialize(@Nullable Send<Buffer> serialized) throws SerializationException {
		try (serialized) {
			var deserialized = fixedLengthSerializer.deserialize(serialized);
			return new DeserializationResult<>(keyMapper.map(deserialized.deserializedData()), deserialized.bytesRead());
		}
	}

	@Override
	public @Nullable Send<Buffer> serialize(@NotNull B deserialized) throws SerializationException {
		return fixedLengthSerializer.serialize(keyMapper.unmap(deserialized));
	}

	@Override
	public int getSerializedBinaryLength() {
		return fixedLengthSerializer.getSerializedBinaryLength();
	}
}
