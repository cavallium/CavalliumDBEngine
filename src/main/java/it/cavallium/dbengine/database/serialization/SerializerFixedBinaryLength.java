package it.cavallium.dbengine.database.serialization;

import io.net5.buffer.api.Buffer;
import io.net5.buffer.api.BufferAllocator;
import io.net5.buffer.api.Send;
import it.cavallium.dbengine.database.LLUtils;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

@SuppressWarnings("unused")
public interface SerializerFixedBinaryLength<A> extends Serializer<A> {

	int getSerializedBinaryLength();

	static SerializerFixedBinaryLength<Send<Buffer>> noop(int length) {
		return new SerializerFixedBinaryLength<>() {
			@Override
			public @NotNull DeserializationResult<Send<Buffer>> deserialize(@NotNull Send<Buffer> serialized) {
				Objects.requireNonNull(serialized);
				try (var buf = serialized.receive()) {
					if (buf.readableBytes() != getSerializedBinaryLength()) {
						throw new IllegalArgumentException(
								"Fixed serializer with " + getSerializedBinaryLength() + " bytes has tried to deserialize an element with "
										+ buf.readableBytes() + " bytes instead");
					}
					var readableBytes = buf.readableBytes();
					return new DeserializationResult<>(buf.send(), readableBytes);
				}
			}

			@Override
			public @NotNull Send<Buffer> serialize(@NotNull Send<Buffer> deserialized) {
				try (Buffer buf = deserialized.receive()) {
					if (buf.readableBytes() != getSerializedBinaryLength()) {
						throw new IllegalArgumentException(
								"Fixed serializer with " + getSerializedBinaryLength() + " bytes has tried to serialize an element with "
										+ buf.readableBytes() + " bytes instead");
					}
					return buf.send();
				}
			}

			@Override
			public int getSerializedBinaryLength() {
				return length;
			}
		};
	}

	static SerializerFixedBinaryLength<String> utf8(BufferAllocator allocator, int length) {
		return new SerializerFixedBinaryLength<>() {
			@Override
			public @NotNull DeserializationResult<String> deserialize(@NotNull Send<Buffer> serializedToReceive)
					throws SerializationException {
				try (var serialized = serializedToReceive.receive()) {
					if (serialized.readableBytes() != getSerializedBinaryLength()) {
						throw new SerializationException(
								"Fixed serializer with " + getSerializedBinaryLength() + " bytes has tried to deserialize an element with "
										+ serialized.readableBytes() + " bytes instead");
					}
					var readerOffset = serialized.readerOffset();
					return new DeserializationResult<>(LLUtils.deserializeString(serialized.send(),
							readerOffset, length, StandardCharsets.UTF_8), length);
				}
			}

			@Override
			public @NotNull Send<Buffer> serialize(@NotNull String deserialized) throws SerializationException {
				// UTF-8 uses max. 3 bytes per char, so calculate the worst case.
				try (Buffer buf = allocator.allocate(LLUtils.utf8MaxBytes(deserialized))) {
					assert buf.isAccessible();
					var bytes = deserialized.getBytes(StandardCharsets.UTF_8);
					buf.ensureWritable(bytes.length);
					buf.writeBytes(bytes);
					if (buf.readableBytes() != getSerializedBinaryLength()) {
						throw new SerializationException("Fixed serializer with " + getSerializedBinaryLength()
								+ " bytes has tried to serialize an element with "
								+ buf.readableBytes() + " bytes instead");
					}
					assert buf.isAccessible();
					return buf.send();
				}
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
			public @NotNull DeserializationResult<Integer> deserialize(@NotNull Send<Buffer> serializedToReceive) {
				Objects.requireNonNull(serializedToReceive);
				try (var serialized = serializedToReceive.receive()) {
					if (serialized.readableBytes() != getSerializedBinaryLength()) {
						throw new IllegalArgumentException(
								"Fixed serializer with " + getSerializedBinaryLength() + " bytes has tried to deserialize an element with "
										+ serialized.readableBytes() + " bytes instead");
					}
					return new DeserializationResult<>(serialized.readInt(), Integer.BYTES);
				}
			}

			@Override
			public @NotNull Send<Buffer> serialize(@NotNull Integer deserialized) {
				try (Buffer buf = allocator.allocate(Integer.BYTES)) {
					return buf.writeInt(deserialized).send();
				}
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
			public @NotNull DeserializationResult<Long> deserialize(@NotNull Send<Buffer> serializedToReceive) {
				Objects.requireNonNull(serializedToReceive);
				try (var serialized = serializedToReceive.receive()) {
					if (serialized.readableBytes() != getSerializedBinaryLength()) {
						throw new IllegalArgumentException(
								"Fixed serializer with " + getSerializedBinaryLength() + " bytes has tried to deserialize an element with "
										+ serialized.readableBytes() + " bytes instead");
					}
					var readableBytes = serialized.readableBytes();
					return new DeserializationResult<>(serialized.readLong(), Long.BYTES);
				}
			}

			@Override
			public @NotNull Send<Buffer> serialize(@NotNull Long deserialized) {
				try (Buffer buf = allocator.allocate(Long.BYTES)) {
					return buf.writeLong(deserialized).send();
				}
			}

			@Override
			public int getSerializedBinaryLength() {
				return Long.BYTES;
			}
		};
	}
}
