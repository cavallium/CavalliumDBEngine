package it.cavallium.dbengine.client;

import io.netty.buffer.api.Buffer;
import it.cavallium.dbengine.database.serialization.SerializationException;
import it.cavallium.dbengine.database.serialization.Serializer;
import org.jetbrains.annotations.NotNull;

public class MappedSerializer<A, B> implements Serializer<B, Buffer> {

	private final Serializer<A, Buffer> serializer;
	private final Mapper<A, B> keyMapper;

	public MappedSerializer(Serializer<A, Buffer> serializer,
			Mapper<A, B> keyMapper) {
		this.serializer = serializer;
		this.keyMapper = keyMapper;
	}

	@Override
	public @NotNull B deserialize(@NotNull Buffer serialized) throws SerializationException {
		try {
			return keyMapper.map(serializer.deserialize(serialized.retain()));
		} finally {
			serialized.release();
		}
	}

	@Override
	public @NotNull Buffer serialize(@NotNull B deserialized) throws SerializationException {
		return serializer.serialize(keyMapper.unmap(deserialized));
	}
}
