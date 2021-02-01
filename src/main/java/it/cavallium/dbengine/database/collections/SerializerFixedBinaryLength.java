package it.cavallium.dbengine.database.collections;

public interface SerializerFixedBinaryLength<A, B> extends Serializer<A, B> {

	int getSerializedBinaryLength();

	static SerializerFixedBinaryLength<byte[], byte[]> noop(int length) {
		return new SerializerFixedBinaryLength<>() {
			@Override
			public byte[] deserialize(byte[] serialized) {
				assert serialized.length == getSerializedBinaryLength();
				return serialized;
			}

			@Override
			public byte[] serialize(byte[] deserialized) {
				assert deserialized.length == getSerializedBinaryLength();
				return deserialized;
			}

			@Override
			public int getSerializedBinaryLength() {
				return length;
			}
		};
	}
}
