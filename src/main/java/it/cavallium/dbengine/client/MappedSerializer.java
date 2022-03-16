package it.cavallium.dbengine.client;

import io.netty5.buffer.api.Buffer;
import io.netty5.buffer.api.Send;
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
	public @NotNull B deserialize(@NotNull Buffer serialized) throws SerializationException {
		return keyMapper.map(serializer.deserialize(serialized));
	}

	@Override
	public void serialize(@NotNull B deserialized, Buffer output) throws SerializationException {
		serializer.serialize(keyMapper.unmap(deserialized), output);
	}

	@Override
	public int getSerializedSizeHint() {
		return serializer.getSerializedSizeHint();
	}
}
