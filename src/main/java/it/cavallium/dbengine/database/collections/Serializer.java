package it.cavallium.dbengine.database.collections;

public interface Serializer<A, B> {

	A deserialize(B serialized);

	B serialize(A deserialized);

	static Serializer<byte[], byte[]> noop() {
		return new Serializer<>() {
			@Override
			public byte[] deserialize(byte[] serialized) {
				return serialized;
			}

			@Override
			public byte[] serialize(byte[] deserialized) {
				return deserialized;
			}
		};
	}
}
