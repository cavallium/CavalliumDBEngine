package it.cavallium.dbengine.database.collections;

import io.netty.buffer.ByteBuf;

public interface FixedLengthSerializer<B> {

	B deserialize(ByteBuf serialized);

	void serialize(B deserialized, ByteBuf output);

	int getLength();

	static FixedLengthSerializer<ByteBuf> noop(int length) {
		return new FixedLengthSerializer<>() {
			@Override
			public ByteBuf deserialize(ByteBuf serialized) {
				return serialized.readSlice(length);
			}

			@Override
			public void serialize(ByteBuf deserialized, ByteBuf output) {
				output.writeBytes(deserialized, length);
			}

			@Override
			public int getLength() {
				return length;
			}
		};
	}
}
