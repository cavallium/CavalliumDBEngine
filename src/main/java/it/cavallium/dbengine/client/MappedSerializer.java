package it.cavallium.dbengine.client;

import it.cavallium.buffer.Buf;
import it.cavallium.buffer.BufDataInput;
import it.cavallium.buffer.BufDataOutput;
import it.cavallium.dbengine.database.serialization.SerializationException;
import it.cavallium.dbengine.database.serialization.Serializer;
import it.cavallium.dbengine.database.serialization.SerializerFixedBinaryLength;
import org.jetbrains.annotations.NotNull;

public class MappedSerializer<A, B> implements Serializer<B> {

	private final Serializer<A> serializer;
	private final Mapper<A, B> keyMapper;

	public MappedSerializer(Serializer<A> serializer,
			Mapper<A, B> keyMapper) {
		this.serializer = serializer;
		this.keyMapper = keyMapper;
	}

	public static <A, B> Serializer<B> of(Serializer<A> ser,
			Mapper<A, B> keyMapper) {
		if (keyMapper.getClass() == NoMapper.class) {
			//noinspection unchecked
			return (Serializer<B>) ser;
		} else {
			return new MappedSerializer<>(ser, keyMapper);
		}
	}

	@Override
	public @NotNull B deserialize(@NotNull BufDataInput in) throws SerializationException {
		return keyMapper.map(serializer.deserialize(in));
	}

	@Override
	public void serialize(@NotNull B deserialized, BufDataOutput out) throws SerializationException {
		serializer.serialize(keyMapper.unmap(deserialized), out);
	}

	@Override
	public int getSerializedSizeHint() {
		return serializer.getSerializedSizeHint();
	}
}
