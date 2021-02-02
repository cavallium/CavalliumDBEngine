package it.cavallium.dbengine.database.serialization;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import org.jetbrains.annotations.NotNull;

public interface SerializerFixedBinaryLength<A, B> extends Serializer<A, B> {

	int getSerializedBinaryLength();

	static SerializerFixedBinaryLength<byte[], byte[]> noop(int length) {
		return new SerializerFixedBinaryLength<>() {
			@Override
			public byte @NotNull [] deserialize(byte @NotNull [] serialized) {
				assert serialized.length == getSerializedBinaryLength();
				return serialized;
			}

			@Override
			public byte @NotNull [] serialize(byte @NotNull [] deserialized) {
				assert deserialized.length == getSerializedBinaryLength();
				return deserialized;
			}

			@Override
			public int getSerializedBinaryLength() {
				return length;
			}
		};
	}

	static SerializerFixedBinaryLength<Integer, byte[]> intSerializer() {
		return new SerializerFixedBinaryLength<>() {
			@Override
			public @NotNull Integer deserialize(byte @NotNull [] serialized) {
				assert serialized.length == getSerializedBinaryLength();
				return Ints.fromByteArray(serialized);
			}

			@Override
			public byte @NotNull [] serialize(@NotNull Integer deserialized) {
				return Ints.toByteArray(deserialized);
			}

			@Override
			public int getSerializedBinaryLength() {
				return Integer.BYTES;
			}
		};
	}

	static SerializerFixedBinaryLength<Long, byte[]> longSerializer() {
		return new SerializerFixedBinaryLength<>() {
			@Override
			public @NotNull Long deserialize(byte @NotNull [] serialized) {
				assert serialized.length == getSerializedBinaryLength();
				return Longs.fromByteArray(serialized);
			}

			@Override
			public byte @NotNull [] serialize(@NotNull Long deserialized) {
				return Longs.toByteArray(deserialized);
			}

			@Override
			public int getSerializedBinaryLength() {
				return Long.BYTES;
			}
		};
	}
}
