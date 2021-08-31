package it.cavallium.dbengine.client;

import io.netty.buffer.api.Buffer;
import io.netty.buffer.api.Send;
import it.cavallium.dbengine.database.serialization.SerializationException;
import it.cavallium.dbengine.database.serialization.SerializerFixedBinaryLength;
import org.jetbrains.annotations.NotNull;

public class MappedSerializerFixedLength<A, B> implements SerializerFixedBinaryLength<B, Send<Buffer>> {

	private final SerializerFixedBinaryLength<A, Send<Buffer>> fixedLengthSerializer;
	private final Mapper<A, B> keyMapper;

	public MappedSerializerFixedLength(SerializerFixedBinaryLength<A, Send<Buffer>> fixedLengthSerializer,
			Mapper<A, B> keyMapper) {
		this.fixedLengthSerializer = fixedLengthSerializer;
		this.keyMapper = keyMapper;
	}

	@Override
	public @NotNull B deserialize(@NotNull Send<Buffer> serialized) throws SerializationException {
		try (serialized) {
			return keyMapper.map(fixedLengthSerializer.deserialize(serialized));
		}
	}

	@Override
	public @NotNull Send<Buffer> serialize(@NotNull B deserialized) throws SerializationException {
		return fixedLengthSerializer.serialize(keyMapper.unmap(deserialized));
	}

	@Override
	public int getSerializedBinaryLength() {
		return fixedLengthSerializer.getSerializedBinaryLength();
	}
}
