package it.cavallium.dbengine.client;

import io.netty.buffer.api.Buffer;
import io.netty.buffer.api.Send;
import it.cavallium.dbengine.database.serialization.SerializationException;
import it.cavallium.dbengine.database.serialization.Serializer;
import org.jetbrains.annotations.NotNull;

public class MappedSerializer<A, B> implements Serializer<B, Send<Buffer>> {

	private final Serializer<A, Send<Buffer>> serializer;
	private final Mapper<A, B> keyMapper;

	public MappedSerializer(Serializer<A, Send<Buffer>> serializer,
			Mapper<A, B> keyMapper) {
		this.serializer = serializer;
		this.keyMapper = keyMapper;
	}

	@Override
	public @NotNull B deserialize(@NotNull Send<Buffer> serialized) throws SerializationException {
		try (serialized) {
			return keyMapper.map(serializer.deserialize(serialized));
		}
	}

	@Override
	public @NotNull Send<Buffer> serialize(@NotNull B deserialized) throws SerializationException {
		return serializer.serialize(keyMapper.unmap(deserialized));
	}
}
