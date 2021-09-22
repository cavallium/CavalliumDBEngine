package it.cavallium.dbengine.client;

import io.net5.buffer.api.Buffer;
import io.net5.buffer.api.Send;
import it.cavallium.dbengine.database.serialization.SerializationException;
import it.cavallium.dbengine.database.serialization.Serializer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class MappedSerializer<A, B> implements Serializer<B> {

	private final Serializer<A> serializer;
	private final Mapper<A, B> keyMapper;

	public MappedSerializer(Serializer<A> serializer,
			Mapper<A, B> keyMapper) {
		this.serializer = serializer;
		this.keyMapper = keyMapper;
	}

	@Override
	public @NotNull DeserializationResult<B> deserialize(@Nullable Send<Buffer> serialized) throws SerializationException {
		try (serialized) {
			var deserialized = serializer.deserialize(serialized);
			return new DeserializationResult<>(keyMapper.map(deserialized.deserializedData()), deserialized.bytesRead());
		}
	}

	@Override
	public @Nullable Send<Buffer> serialize(@NotNull B deserialized) throws SerializationException {
		return serializer.serialize(keyMapper.unmap(deserialized));
	}
}
