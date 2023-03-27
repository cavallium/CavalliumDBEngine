package it.cavallium.dbengine.database.disk;

import it.cavallium.dbengine.database.Delta;
import it.cavallium.dbengine.database.UpdateReturnMode;
import it.cavallium.dbengine.database.serialization.SerializationException;
import it.cavallium.dbengine.database.serialization.SerializationFunction;
import java.util.function.Function;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class CachedSerializationFunction<U, A, B> implements SerializationFunction<A, B> {

	private final SerializationFunction<U, U> updater;
	private final Function<U, B> serializer;
	private final Function<A, U> deserializer;

	private @Nullable U prevDeserialized;
	private @Nullable U currDeserialized;

	public CachedSerializationFunction(SerializationFunction<@Nullable U, @Nullable U> updater,
			Function<@NotNull U, @Nullable B> serializer,
			Function<@NotNull A, @Nullable U> deserializer) {
		this.updater = updater;
		this.serializer = serializer;
		this.deserializer = deserializer;
	}

	@Override
	public @Nullable B apply(@Nullable A oldSerialized) throws SerializationException {
		U result;
		U old;
		if (oldSerialized == null) {
			old = null;
		} else {
			old = deserializer.apply(oldSerialized);
		}
		this.prevDeserialized = old;
		result = updater.apply(old);
		this.currDeserialized = result;
		if (result == null) {
			return null;
		} else {
			return serializer.apply(result);
		}
	}

	public @Nullable U getPrevDeserialized() {
		return prevDeserialized;
	}

	public @Nullable U getCurrDeserialized() {
		return currDeserialized;
	}

	public Delta<U> getDelta() {
		return new Delta<>(prevDeserialized, currDeserialized);
	}

	public U getResult(UpdateReturnMode updateReturnMode) {
		return switch (updateReturnMode) {
			case GET_NEW_VALUE -> currDeserialized;
			case GET_OLD_VALUE -> prevDeserialized;
			case NOTHING -> null;
		};
	}
}
