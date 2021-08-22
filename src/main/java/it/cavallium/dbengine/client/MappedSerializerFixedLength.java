package it.cavallium.dbengine.client;

import io.netty.buffer.ByteBuf;
import it.cavallium.dbengine.database.serialization.SerializationException;
import it.cavallium.dbengine.database.serialization.SerializerFixedBinaryLength;
import org.jetbrains.annotations.NotNull;

public class MappedSerializerFixedLength<A, B> implements SerializerFixedBinaryLength<B, ByteBuf> {

	private final SerializerFixedBinaryLength<A, ByteBuf> fixedLengthSerializer;
	private final Mapper<A, B> keyMapper;

	public MappedSerializerFixedLength(SerializerFixedBinaryLength<A, ByteBuf> fixedLengthSerializer,
			Mapper<A, B> keyMapper) {
		this.fixedLengthSerializer = fixedLengthSerializer;
		this.keyMapper = keyMapper;
	}

	@Override
	public @NotNull B deserialize(@NotNull ByteBuf serialized) throws SerializationException {
		try {
			return keyMapper.map(fixedLengthSerializer.deserialize(serialized.retain()));
		} finally {
			serialized.release();
		}
	}

	@Override
	public @NotNull ByteBuf serialize(@NotNull B deserialized) throws SerializationException {
		return fixedLengthSerializer.serialize(keyMapper.unmap(deserialized));
	}

	@Override
	public int getSerializedBinaryLength() {
		return fixedLengthSerializer.getSerializedBinaryLength();
	}
}
