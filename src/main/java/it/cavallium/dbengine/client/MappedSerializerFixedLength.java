package it.cavallium.dbengine.client;

import it.cavallium.dbengine.buffers.Buf;
import it.cavallium.dbengine.buffers.BufDataInput;
import it.cavallium.dbengine.buffers.BufDataOutput;
import it.cavallium.dbengine.database.serialization.SerializationException;
import it.cavallium.dbengine.database.serialization.SerializerFixedBinaryLength;
import org.jetbrains.annotations.NotNull;

public class MappedSerializerFixedLength<A, B> implements SerializerFixedBinaryLength<B> {

	private final SerializerFixedBinaryLength<A> fixedLengthSerializer;
	private final Mapper<A, B> keyMapper;

	public MappedSerializerFixedLength(SerializerFixedBinaryLength<A> fixedLengthSerializer,
			Mapper<A, B> keyMapper) {
		this.fixedLengthSerializer = fixedLengthSerializer;
		this.keyMapper = keyMapper;
	}

	@Override
	public @NotNull B deserialize(@NotNull BufDataInput in) throws SerializationException {
		return keyMapper.map(fixedLengthSerializer.deserialize(in));
	}

	@Override
	public void serialize(@NotNull B deserialized, BufDataOutput out) throws SerializationException {
		fixedLengthSerializer.serialize(keyMapper.unmap(deserialized), out);
	}

	@Override
	public int getSerializedBinaryLength() {
		return fixedLengthSerializer.getSerializedBinaryLength();
	}
}
