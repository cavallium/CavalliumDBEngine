package it.cavallium.dbengine.database.collections;

import io.net5.buffer.api.Buffer;
import io.net5.buffer.api.BufferAllocator;
import io.net5.buffer.api.CompositeBuffer;
import io.net5.buffer.api.Send;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.serialization.SerializationException;
import it.cavallium.dbengine.database.serialization.Serializer;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

class ValueWithHashSerializer<X, Y> implements Serializer<Entry<X, Y>> {

	private final Serializer<X> keySuffixSerializer;
	private final Serializer<Y> valueSerializer;

	ValueWithHashSerializer(
			Serializer<X> keySuffixSerializer,
			Serializer<Y> valueSerializer) {
		this.keySuffixSerializer = keySuffixSerializer;
		this.valueSerializer = valueSerializer;
	}

	@Override
	public @NotNull Entry<X, Y> deserialize(@NotNull Buffer serialized) throws SerializationException {
		Objects.requireNonNull(serialized);
		X deserializedKey = keySuffixSerializer.deserialize(serialized);
		Y deserializedValue = valueSerializer.deserialize(serialized);
		return Map.entry(deserializedKey, deserializedValue);
	}

	@Override
	public void serialize(@NotNull Entry<X, Y> deserialized, Buffer output) throws SerializationException {
		keySuffixSerializer.serialize(deserialized.getKey(), output);
		valueSerializer.serialize(deserialized.getValue(), output);
	}

	@Override
	public int getSerializedSizeHint() {
		var hint1 = keySuffixSerializer.getSerializedSizeHint();
		var hint2 = valueSerializer.getSerializedSizeHint();
		if (hint1 == -1 && hint2 == -1) {
			return -1;
		} else if (hint1 == -1) {
			return hint2;
		} else {
			return hint1;
		}
	}
}
