package it.cavallium.dbengine.database.collections;

import io.netty.buffer.ByteBuf;

public interface SerializerFixedBinaryLength<A, B> extends Serializer<A, B> {

	int getSerializedBinaryLength();

	static SerializerFixedBinaryLength<ByteBuf, ByteBuf> noop(int length) {
		return new SerializerFixedBinaryLength<>() {
			@Override
			public ByteBuf deserialize(ByteBuf serialized) {
				return serialized.readSlice(length);
			}

			@Override
			public void serialize(ByteBuf deserialized, ByteBuf output) {
				output.writeBytes(deserialized.slice(), length);
			}

			@Override
			public int getSerializedBinaryLength() {
				return length;
			}
		};
	}
}
