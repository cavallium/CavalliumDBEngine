package it.cavallium.dbengine.database.collections;

import it.cavallium.buffer.BufDataInput;
import it.cavallium.buffer.BufDataOutput;
import it.cavallium.dbengine.database.serialization.SerializationException;
import it.cavallium.dbengine.database.serialization.Serializer;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import org.jetbrains.annotations.NotNull;

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
	public @NotNull Entry<X, Y> deserialize(@NotNull BufDataInput in) throws SerializationException {
		Objects.requireNonNull(in);
		X deserializedKey = keySuffixSerializer.deserialize(in);
		Y deserializedValue = valueSerializer.deserialize(in);
		return Map.entry(deserializedKey, deserializedValue);
	}

	@Override
	public void serialize(@NotNull Entry<X, Y> deserialized, BufDataOutput out) throws SerializationException {
		keySuffixSerializer.serialize(deserialized.getKey(), out);
		valueSerializer.serialize(deserialized.getValue(), out);
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
