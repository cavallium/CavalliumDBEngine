package it.cavallium.dbengine.database.serialization;

import it.cavallium.dbengine.buffers.Buf;
import it.cavallium.dbengine.buffers.BufDataInput;
import it.cavallium.dbengine.buffers.BufDataOutput;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import org.jetbrains.annotations.NotNull;

@SuppressWarnings("unused")
public interface SerializerFixedBinaryLength<A> extends Serializer<A> {

	int getSerializedBinaryLength();

	@Override
	default int getSerializedSizeHint() {
		return getSerializedBinaryLength();
	}

	static SerializerFixedBinaryLength<Buf> noop(int length) {
		return new SerializerFixedBinaryLength<>() {
			@Override
			public @NotNull Buf deserialize(@NotNull BufDataInput in) throws SerializationException {
				Objects.requireNonNull(in);
				if (in.available() < getSerializedBinaryLength()) {
					throw new IllegalArgumentException(
							"Fixed serializer with " + getSerializedBinaryLength() + " bytes has tried to deserialize an element with "
									+ in.available() + " bytes instead");
				}
				return Buf.wrap(in.readNBytes(getSerializedBinaryLength()));
			}

			@Override
			public void serialize(@NotNull Buf deserialized, BufDataOutput out) throws SerializationException {
				if (deserialized.size() != getSerializedBinaryLength()) {
					throw new IllegalArgumentException(
							"Fixed serializer with " + getSerializedBinaryLength() + " bytes has tried to serialize an element with "
									+ deserialized.size() + " bytes instead");
				}
				out.writeBytes(deserialized);
			}

			@Override
			public int getSerializedBinaryLength() {
				return length;
			}
		};
	}

	static SerializerFixedBinaryLength<String> utf8(int length) {
		return new SerializerFixedBinaryLength<>() {

			@Override
			public @NotNull String deserialize(@NotNull BufDataInput in) throws SerializationException {
				if (in.available() < getSerializedBinaryLength()) {
					throw new SerializationException(
							"Fixed serializer with " + getSerializedBinaryLength() + " bytes has tried to deserialize an element with "
									+ in.available() + " bytes instead");
				}
				return new String(in.readNBytes(length), StandardCharsets.UTF_8);
			}

			@Override
			public void serialize(@NotNull String deserialized, BufDataOutput out) throws SerializationException {
				var bytes = deserialized.getBytes(StandardCharsets.UTF_8);
				out.ensureWritable(bytes.length);
				out.write(bytes);
				if (bytes.length < getSerializedBinaryLength()) {
					throw new SerializationException("Fixed serializer with " + getSerializedBinaryLength()
							+ " bytes has tried to serialize an element with "
							+ bytes.length + " bytes instead");
				}
			}

			@Override
			public int getSerializedBinaryLength() {
				return length;
			}
		};
	}

	static SerializerFixedBinaryLength<Integer> intSerializer() {
		return new SerializerFixedBinaryLength<>() {

			@Override
			public @NotNull Integer deserialize(@NotNull BufDataInput in) throws SerializationException {
				Objects.requireNonNull(in);
				if (in.available() < getSerializedBinaryLength()) {
					throw new IllegalArgumentException(
							"Fixed serializer with " + getSerializedBinaryLength() + " bytes has tried to deserialize an element with "
									+ in.available() + " bytes instead");
				}
				return in.readInt();
			}

			@Override
			public void serialize(@NotNull Integer deserialized, BufDataOutput out) throws SerializationException {
				out.writeInt(deserialized);
			}

			@Override
			public int getSerializedBinaryLength() {
				return Integer.BYTES;
			}
		};
	}

	static SerializerFixedBinaryLength<Long> longSerializer() {
		return new SerializerFixedBinaryLength<>() {

			@Override
			public @NotNull Long deserialize(@NotNull BufDataInput in) throws SerializationException {
				Objects.requireNonNull(in);
				if (in.available() < getSerializedBinaryLength()) {
					throw new IllegalArgumentException(
							"Fixed serializer with " + getSerializedBinaryLength() + " bytes has tried to deserialize an element with "
									+ in.available() + " bytes instead");
				}
				return in.readLong();
			}

			@Override
			public void serialize(@NotNull Long deserialized, BufDataOutput out) throws SerializationException {
				out.writeLong(deserialized);
			}

			@Override
			public int getSerializedBinaryLength() {
				return Long.BYTES;
			}
		};
	}
}
