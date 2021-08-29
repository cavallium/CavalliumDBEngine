package it.cavallium.dbengine.client;

import io.netty.buffer.api.Buffer;
import it.cavallium.dbengine.database.serialization.SerializationException;
import it.cavallium.dbengine.database.serialization.SerializerFixedBinaryLength;
import org.jetbrains.annotations.NotNull;

public class MappedSerializerFixedLength<A, B> implements SerializerFixedBinaryLength<B, Buffer> {

	private final SerializerFixedBinaryLength<A, Buffer> fixedLengthSerializer;
	private final Mapper<A, B> keyMapper;

	public MappedSerializerFixedLength(SerializerFixedBinaryLength<A, Buffer> fixedLengthSerializer,
			Mapper<A, B> keyMapper) {
		this.fixedLengthSerializer = fixedLengthSerializer;
		this.keyMapper = keyMapper;
	}

	@Override
	public @NotNull B deserialize(@NotNull Buffer serialized) throws SerializationException {
		try {
			return keyMapper.map(fixedLengthSerializer.deserialize(serialized.retain()));
		} finally {
			serialized.release();
		}
	}

	@Override
	public @NotNull Buffer serialize(@NotNull B deserialized) throws SerializationException {
		return fixedLengthSerializer.serialize(keyMapper.unmap(deserialized));
	}

	@Override
	public int getSerializedBinaryLength() {
		return fixedLengthSerializer.getSerializedBinaryLength();
	}
}
