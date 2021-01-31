package it.cavallium.dbengine.database.collections;

import io.netty.buffer.ByteBuf;

public interface FixedLengthSerializer<B> {

	B deserialize(ByteBuf serialized, int length);

	void serialize(B deserialized, ByteBuf output, int length);

	static FixedLengthSerializer<ByteBuf> noop() {
		return new FixedLengthSerializer<>() {
			@Override
			public ByteBuf deserialize(ByteBuf serialized, int length) {
				return serialized.readSlice(length);
			}

			@Override
			public void serialize(ByteBuf deserialized, ByteBuf output, int length) {
				output.writeBytes(deserialized, length);
			}
		};
	}
}
