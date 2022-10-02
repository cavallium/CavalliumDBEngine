package it.cavallium.dbengine.client;

import io.netty5.buffer.Buffer;
import io.netty5.util.Send;
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
	public @NotNull B deserialize(@NotNull Buffer serialized) throws SerializationException {
		return keyMapper.map(fixedLengthSerializer.deserialize(serialized));
	}

	@Override
	public void serialize(@NotNull B deserialized, Buffer output) throws SerializationException {
		fixedLengthSerializer.serialize(keyMapper.unmap(deserialized), output);
	}

	@Override
	public int getSerializedBinaryLength() {
		return fixedLengthSerializer.getSerializedBinaryLength();
	}
}
