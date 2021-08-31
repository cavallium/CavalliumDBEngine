package it.cavallium.dbengine.database.serialization;

import io.netty.buffer.api.Buffer;
import io.netty.buffer.api.BufferAllocator;
import io.netty.buffer.api.Send;
import it.cavallium.dbengine.database.LLUtils;
import java.nio.charset.StandardCharsets;
import org.jetbrains.annotations.NotNull;

@SuppressWarnings("unused")
public interface SerializerFixedBinaryLength<A, B> extends Serializer<A, B> {

	int getSerializedBinaryLength();

	static SerializerFixedBinaryLength<Send<Buffer>, Send<Buffer>> noop(int length) {
		return new SerializerFixedBinaryLength<>() {
			@Override
			public @NotNull Send<Buffer> deserialize(@NotNull Send<Buffer> serialized) {
				try (var buf = serialized.receive()) {
					if (buf.readableBytes() != getSerializedBinaryLength()) {
						throw new IllegalArgumentException(
								"Fixed serializer with " + getSerializedBinaryLength() + " bytes has tried to deserialize an element with "
										+ buf.readableBytes() + " bytes instead");
					}
					return buf.send();
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

	static SerializerFixedBinaryLength<String, Send<Buffer>> utf8(BufferAllocator allocator, int length) {
		return new SerializerFixedBinaryLength<>() {
			@Override
			public @NotNull String deserialize(@NotNull Send<Buffer> serializedToReceive) throws SerializationException {
				try (var serialized = serializedToReceive.receive()) {
					if (serialized.readableBytes() != getSerializedBinaryLength()) {
						throw new SerializationException(
								"Fixed serializer with " + getSerializedBinaryLength() + " bytes has tried to deserialize an element with "
										+ serialized.readableBytes() + " bytes instead");
					}
					var readerOffset = serialized.readerOffset();
					return LLUtils.deserializeString(serialized.send(), readerOffset, length, StandardCharsets.UTF_8);
				}
			}

			@Override
			public @NotNull Send<Buffer> serialize(@NotNull String deserialized) throws SerializationException {
				// UTF-8 uses max. 3 bytes per char, so calculate the worst case.
				try (Buffer buf = allocator.allocate(LLUtils.utf8MaxBytes(deserialized))) {
					buf.writeBytes(deserialized.getBytes(StandardCharsets.UTF_8));
					if (buf.readableBytes() != getSerializedBinaryLength()) {
						throw new SerializationException("Fixed serializer with " + getSerializedBinaryLength()
								+ " bytes has tried to serialize an element with "
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

	static SerializerFixedBinaryLength<Integer, Send<Buffer>> intSerializer(BufferAllocator allocator) {
		return new SerializerFixedBinaryLength<>() {
			@Override
			public @NotNull Integer deserialize(@NotNull Send<Buffer> serializedToReceive) {
				try (var serialized = serializedToReceive.receive()) {
					if (serialized.readableBytes() != getSerializedBinaryLength()) {
						throw new IllegalArgumentException(
								"Fixed serializer with " + getSerializedBinaryLength() + " bytes has tried to deserialize an element with "
										+ serialized.readableBytes() + " bytes instead");
					}
					return serialized.readInt();
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

	static SerializerFixedBinaryLength<Long, Send<Buffer>> longSerializer(BufferAllocator allocator) {
		return new SerializerFixedBinaryLength<>() {
			@Override
			public @NotNull Long deserialize(@NotNull Send<Buffer> serializedToReceive) {
				try (var serialized = serializedToReceive.receive()) {
					if (serialized.readableBytes() != getSerializedBinaryLength()) {
						throw new IllegalArgumentException(
								"Fixed serializer with " + getSerializedBinaryLength() + " bytes has tried to deserialize an element with "
										+ serialized.readableBytes() + " bytes instead");
					}
					return serialized.readLong();
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
