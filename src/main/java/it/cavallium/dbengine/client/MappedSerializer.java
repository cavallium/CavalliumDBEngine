package it.cavallium.dbengine.client;

import io.netty.buffer.api.Buffer;
import io.netty.buffer.api.Send;
import it.cavallium.dbengine.database.serialization.SerializationException;
import it.cavallium.dbengine.database.serialization.Serializer;
import org.jetbrains.annotations.NotNull;

public class MappedSerializer<A, B> implements Serializer<B> {

	private final Serializer<A> serializer;
	private final Mapper<A, B> keyMapper;

	public MappedSerializer(Serializer<A> serializer,
			Mapper<A, B> keyMapper) {
		this.serializer = serializer;
		this.keyMapper = keyMapper;
	}

	@Override
	public @NotNull DeserializationResult<B> deserialize(@NotNull Send<Buffer> serialized) throws SerializationException {
		try (serialized) {
			var deserialized = serializer.deserialize(serialized);
			return new DeserializationResult<>(keyMapper.map(deserialized.deserializedData()), deserialized.bytesRead());
		}
	}

	@Override
	public @NotNull Send<Buffer> serialize(@NotNull B deserialized) throws SerializationException {
		return serializer.serialize(keyMapper.unmap(deserialized));
	}
}
