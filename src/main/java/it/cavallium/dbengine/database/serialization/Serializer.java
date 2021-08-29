package it.cavallium.dbengine.database.serialization;

import io.netty.buffer.api.Buffer;
import io.netty.buffer.api.BufferAllocator;
import io.netty.buffer.api.Send;
import it.cavallium.dbengine.database.LLUtils;
import java.nio.charset.StandardCharsets;
import org.jetbrains.annotations.NotNull;

public interface Serializer<A, B> {

	@NotNull A deserialize(@NotNull B serialized) throws SerializationException;

	@NotNull B serialize(@NotNull A deserialized) throws SerializationException;

	Serializer<Send<Buffer>, Send<Buffer>> NOOP_SERIALIZER = new Serializer<>() {
		@Override
		public @NotNull Send<Buffer> deserialize(@NotNull Send<Buffer> serialized) {
			return serialized;
		}

		@Override
		public @NotNull Send<Buffer> serialize(@NotNull Send<Buffer> deserialized) {
			return deserialized;
		}
	};

	static Serializer<Send<Buffer>, Send<Buffer>> noop() {
		return NOOP_SERIALIZER;
	}

	static Serializer<String, Send<Buffer>> utf8(BufferAllocator allocator) {
		return new Serializer<>() {
			@Override
			public @NotNull String deserialize(@NotNull Send<Buffer> serializedToReceive) {
				try (Buffer serialized = serializedToReceive.receive()) {
					int length = serialized.readInt();
					return LLUtils.deserializeString(serialized.send(), serialized.readerOffset(), length, StandardCharsets.UTF_8);
				}
			}

			@Override
			public @NotNull Send<Buffer> serialize(@NotNull String deserialized) {
				// UTF-8 uses max. 3 bytes per char, so calculate the worst case.
				int length = LLUtils.utf8MaxBytes(deserialized);
				try (Buffer buf = allocator.allocate(Integer.BYTES + length)) {
					buf.writeInt(length);
					LLUtils.writeString(buf, deserialized, StandardCharsets.UTF_8);
					return buf.send();
				}
			}
		};
	}
}
