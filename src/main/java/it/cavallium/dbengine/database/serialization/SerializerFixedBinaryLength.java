package it.cavallium.dbengine.database.serialization;

import io.netty5.buffer.Buffer;
import io.netty5.buffer.BufferAllocator;
import io.netty5.util.Send;
import it.cavallium.dbengine.database.LLUtils;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

@SuppressWarnings("unused")
public interface SerializerFixedBinaryLength<A> extends Serializer<A> {

	int getSerializedBinaryLength();

	@Override
	default int getSerializedSizeHint() {
		return getSerializedBinaryLength();
	}

	static SerializerFixedBinaryLength<Buffer> noop(int length) {
		return new SerializerFixedBinaryLength<>() {
			@Override
			public @NotNull Buffer deserialize(@NotNull Buffer serialized) {
				Objects.requireNonNull(serialized);
				if (serialized.readableBytes() < getSerializedBinaryLength()) {
					throw new IllegalArgumentException(
							"Fixed serializer with " + getSerializedBinaryLength() + " bytes has tried to deserialize an element with "
									+ serialized.readableBytes() + " bytes instead");
				}
				return serialized.readSplit(getSerializedBinaryLength());
			}

			@Override
			public void serialize(@NotNull Buffer deserialized, Buffer output) {
				try (deserialized) {
					if (deserialized.readableBytes() != getSerializedBinaryLength()) {
						throw new IllegalArgumentException(
								"Fixed serializer with " + getSerializedBinaryLength() + " bytes has tried to serialize an element with "
										+ deserialized.readableBytes() + " bytes instead");
					}
					output.writeBytes(deserialized);
				}
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
			public @NotNull String deserialize(@NotNull Buffer serialized) throws SerializationException {
				if (serialized.readableBytes() < getSerializedBinaryLength()) {
					throw new SerializationException(
							"Fixed serializer with " + getSerializedBinaryLength() + " bytes has tried to deserialize an element with "
									+ serialized.readableBytes() + " bytes instead");
				}
				var readerOffset = serialized.readerOffset();
				return LLUtils.deserializeString(serialized.send(), readerOffset, length, StandardCharsets.UTF_8);
			}

			@Override
			public void serialize(@NotNull String deserialized, Buffer output) throws SerializationException {
				assert output.isAccessible();
				var bytes = deserialized.getBytes(StandardCharsets.UTF_8);
				output.ensureWritable(bytes.length);
				output.writeBytes(bytes);
				if (output.readableBytes() < getSerializedBinaryLength()) {
					throw new SerializationException("Fixed serializer with " + getSerializedBinaryLength()
							+ " bytes has tried to serialize an element with "
							+ output.readableBytes() + " bytes instead");
				}
				assert output.isAccessible();
			}

			@Override
			public int getSerializedBinaryLength() {
				return length;
			}
		};
	}

	static SerializerFixedBinaryLength<Integer> intSerializer(BufferAllocator allocator) {
		return new SerializerFixedBinaryLength<>() {

			@Override
			public @NotNull Integer deserialize(@NotNull Buffer serialized) {
				Objects.requireNonNull(serialized);
				if (serialized.readableBytes() < getSerializedBinaryLength()) {
					throw new IllegalArgumentException(
							"Fixed serializer with " + getSerializedBinaryLength() + " bytes has tried to deserialize an element with "
									+ serialized.readableBytes() + " bytes instead");
				}
				return serialized.readInt();
			}

			@Override
			public void serialize(@NotNull Integer deserialized, Buffer output) {
				output.writeInt(deserialized);
			}

			@Override
			public int getSerializedBinaryLength() {
				return Integer.BYTES;
			}
		};
	}

	static SerializerFixedBinaryLength<Long> longSerializer(BufferAllocator allocator) {
		return new SerializerFixedBinaryLength<>() {

			@Override
			public @NotNull Long deserialize(@NotNull Buffer serialized) {
				Objects.requireNonNull(serialized);
				if (serialized.readableBytes() < getSerializedBinaryLength()) {
					throw new IllegalArgumentException(
							"Fixed serializer with " + getSerializedBinaryLength() + " bytes has tried to deserialize an element with "
									+ serialized.readableBytes() + " bytes instead");
				}
				return serialized.readLong();
			}

			@Override
			public void serialize(@NotNull Long deserialized, Buffer output) {
				output.writeLong(deserialized);
			}

			@Override
			public int getSerializedBinaryLength() {
				return Long.BYTES;
			}
		};
	}
}
