package it.cavallium.dbengine.client;

import io.netty.buffer.ByteBuf;
import it.cavallium.dbengine.database.serialization.SerializationException;
import it.cavallium.dbengine.database.serialization.Serializer;
import org.jetbrains.annotations.NotNull;

public class MappedSerializer<A, B> implements Serializer<B, ByteBuf> {

	private final Serializer<A, ByteBuf> serializer;
	private final Mapper<A, B> keyMapper;

	public MappedSerializer(Serializer<A, ByteBuf> serializer,
			Mapper<A, B> keyMapper) {
		this.serializer = serializer;
		this.keyMapper = keyMapper;
	}

	@Override
	public @NotNull B deserialize(@NotNull ByteBuf serialized) throws SerializationException {
		try {
			return keyMapper.map(serializer.deserialize(serialized.retain()));
		} finally {
			serialized.release();
		}
	}

	@Override
	public @NotNull ByteBuf serialize(@NotNull B deserialized) throws SerializationException {
		return serializer.serialize(keyMapper.unmap(deserialized));
	}
}
