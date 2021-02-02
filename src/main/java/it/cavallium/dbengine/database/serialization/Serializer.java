package it.cavallium.dbengine.database.serialization;

import org.jetbrains.annotations.NotNull;

public interface Serializer<A, B> {

	@NotNull A deserialize(@NotNull B serialized);

	@NotNull B serialize(@NotNull A deserialized);

	static Serializer<byte[], byte[]> noop() {
		return new Serializer<>() {
			@Override
			public byte @NotNull [] deserialize(byte @NotNull [] serialized) {
				return serialized;
			}

			@Override
			public byte @NotNull [] serialize(byte @NotNull [] deserialized) {
				return deserialized;
			}
		};
	}
}
