package it.cavallium.dbengine.database.serialization;

import io.netty.buffer.api.Buffer;
import io.netty.buffer.api.BufferAllocator;
import io.netty.buffer.api.Send;
import it.cavallium.dbengine.database.LLUtils;
import java.nio.charset.StandardCharsets;
import org.jetbrains.annotations.NotNull;

public interface Serializer<A> {

	record DeserializationResult<T>(T deserializedData, int bytesRead) {}

	@NotNull DeserializationResult<A> deserialize(@NotNull Send<Buffer> serialized) throws SerializationException;

	@NotNull Send<Buffer> serialize(@NotNull A deserialized) throws SerializationException;

	Serializer<Send<Buffer>> NOOP_SERIALIZER = new Serializer<>() {
		@Override
		public @NotNull DeserializationResult<Send<Buffer>> deserialize(@NotNull Send<Buffer> serialized) {
			try (var serializedBuf = serialized.receive()) {
				var readableBytes = serializedBuf.readableBytes();
				return new DeserializationResult<>(serializedBuf.send(), readableBytes);
			}
		}

		@Override
		public @NotNull Send<Buffer> serialize(@NotNull Send<Buffer> deserialized) {
			return deserialized;
		}
	};

	static Serializer<Send<Buffer>> noop() {
		return NOOP_SERIALIZER;
	}

	static Serializer<String> utf8(BufferAllocator allocator) {
		return new Serializer<>() {
			@Override
			public @NotNull DeserializationResult<String> deserialize(@NotNull Send<Buffer> serializedToReceive) {
				try (Buffer serialized = serializedToReceive.receive()) {
					assert serialized.isAccessible();
					int length = serialized.readInt();
					var readerOffset = serialized.readerOffset();
					return new DeserializationResult<>(LLUtils.deserializeString(serialized.send(),
							readerOffset, length, StandardCharsets.UTF_8), Integer.BYTES + length);
				}
			}

			@Override
			public @NotNull Send<Buffer> serialize(@NotNull String deserialized) {
				var bytes = deserialized.getBytes(StandardCharsets.UTF_8);
				try (Buffer buf = allocator.allocate(Integer.BYTES + bytes.length)) {
					assert buf.isAccessible();
					buf.writeInt(bytes.length);
					buf.writeBytes(bytes);
					assert buf.isAccessible();
					return buf.send();
				}
			}
		};
	}
}
